from pydub import AudioSegment, effects
import os
import multiprocessing as mp
try:
    from utils.get_valid_interval import get_valid_interval
    from utils.eda import read_meta_data, add_valid_interval_field
except ModuleNotFoundError:
    from get_valid_interval import get_valid_interval
    from eda import read_meta_data, add_valid_interval_field   
from tqdm import tqdm

def is_song_valid_dur(sound: AudioSegment):
    dur = len(sound) / 1000
    return dur > 0.5 # and dur < 10.2

def is_hum_valid_dur(sound: AudioSegment):
    dur = len(sound) / 1000
    return dur > 0.5 # and dur < 20

def is_valid_sound(sound: AudioSegment, type="song"):
    if ((type == "song" or type == "full_song") and is_song_valid_dur(sound)) \
        or (type == "hum" and is_hum_valid_dur(sound)):
        return True
    return False

def trim_sil(sound: AudioSegment):
    sound_interval = get_valid_interval(sound)
    trimmed_sound  = sound[sound_interval[0]:sound_interval[1]]
    return trimmed_sound

def clean_file(sound_path: str, cleaned_path: str, type="song", check_valid=True, max_dur=None,format:str="mp3"):
    sound = AudioSegment.from_file(sound_path, format)
    trimmed_sound = trim_sil(sound)

    if max_dur is not None:
        trimmed_sound = trimmed_sound[:max_dur]
    if check_valid:
        if not is_valid_sound(trimmed_sound, type):
            return False
    else:
        if not is_valid_sound(trimmed_sound, type):
            trimmed_sound = sound
    

    normalizedsound = effects.normalize(trimmed_sound)          
    normalizedsound.export(cleaned_path, format="mp3")
    return cleaned_path

def _create_data_dict(data_path):
    data_info = read_meta_data(os.path.join(data_path, "train_meta.csv"))
    data_info = add_valid_interval_field(data_info, data_path)
    data_dict = {}
    
    for row in data_info:
        music_id = row[0]
        song_path = row[1]
        hum_path = row[2]
        sound_data = [song_path, hum_path]
        if music_id in data_dict.keys():
            data_dict[music_id].append(sound_data)
        else:
            data_dict[music_id] = [sound_data]

    return data_dict
def clean_train_set(data_path, out_dir):
    os.makedirs(os.path.join(out_dir, "train"),exist_ok=True)
    os.makedirs(os.path.join(out_dir, "train", "song"),exist_ok=True)
    os.makedirs(os.path.join(out_dir, "train", "hum"),exist_ok=True)

    data_dict = _create_data_dict(data_path)
    for music_id in tqdm(data_dict.keys()):
        song_list = data_dict[music_id]
        min_dur = song_list[0][2]
        for song in song_list[1:]:
            min_dur = min(song[2], min_dur)
        
        for song in song_list:
            sound_path = os.path.join(data_path, song[0])
            song_cleaned_path = os.path.join(out_dir, "train", song[0])
            hum_path = os.path.join(data_path, song[1])
            hum_cleaned_path = os.path.join(out_dir, "train", song[1])
            os.makedirs(os.path.dirname(song_cleaned_path), exist_ok=True)
            os.makedirs(os.path.dirname(hum_cleaned_path), exist_ok=True)
            song_res = clean_file(sound_path, song_cleaned_path, type="song", check_valid=True, max_dur=min_dur * 1000, format="mp3")
            hum_res = clean_file(hum_path, hum_cleaned_path, type="hum", check_valid=True, format="wav")

            if isinstance(song_res, str) and isinstance(hum_res, str):
                continue
            else:
                print(f"[INFO] {sound_path} has been removed")
                if isinstance(song_res, str):
                    os.remove(song_res)
                if isinstance(hum_res, str):
                    os.remove(hum_res)

def clean_test_set(data_path, out_dir, test_type="public_test"):
    os.makedirs(os.path.join(out_dir))
    os.makedirs(os.path.join(out_dir, test_type))
    os.makedirs(os.path.join(out_dir, test_type, "full_song"))
    os.makedirs(os.path.join(out_dir, test_type, "hum"))

    subfolder = ["hum", "full_song"]
    for sub in subfolder:
        files = os.listdir(os.path.join(data_path, test_type, sub))
        
        ## complementary: Multi-processing
        print(f'cleaning {sub}... ')
        pool = mp.Pool()
        for file in files:
            if file[-4:] != ".mp3" and file[-4:] != ".wav":
                continue
            # preprocess audio
            sound_path = os.path.join(data_path, test_type, sub, file)
            cleaned_path = os.path.join(out_dir, test_type, sub, file)
            # res = clean_file(sound_path, cleaned_path, type=sub, check_valid=False)
            pool.apply_async(clean_file, args = (sound_path, cleaned_path, sub, False, ))
        pool.close()
        pool.join()
        ##