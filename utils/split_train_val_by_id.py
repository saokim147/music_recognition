import os
import csv
import argparse
import yaml
import random

def move_folder(source_folder, destination_folder):
    for file_name in os.listdir(source_folder):
        # construct full file path
        source = os.path.join(source_folder, file_name)
        destination = os.path.join(destination_folder, file_name)
        # copy only files
        if os.path.isfile(source):
            os.rename(source, destination)

def main(config):
    random.seed(1234)

    indir = config["path"]["preprocessed_path"]
    val_size = config["preprocessing"]["val_size"]

    unique_music = {}
    with open(os.path.join(indir, "train_meta.csv"), "r") as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        line_count = 0
        for row in csv_reader:
            if line_count > 0:
                music_id = row[0]
                song_filename = row[1].split("/")[-1].split(".")[0]  # e.g., 0bc7f9ae9b644f29_2_0d7893612d6572a1
                hum_filename = row[2].split("/")[-1].split(".")[0]   # e.g., hum_0bc7f9ae9b644f29_2_1ded76c36df1388a
                if music_id not in unique_music.keys():
                    unique_music[music_id] = [(song_filename, hum_filename)]
                else:
                    unique_music[music_id].append((song_filename, hum_filename))
            line_count += 1
        print(f'Processed {line_count} lines.')
    val_set = random.sample(list(unique_music.keys()), k=val_size)

    if os.path.isdir(os.path.join(indir, "val")):
        move_folder(os.path.join(indir, "val", "song"), os.path.join(indir, "train", "song"))
        move_folder(os.path.join(indir, "val", "hum"), os.path.join(indir, "train", "hum"))
    else:
        os.makedirs(os.path.join(indir, "val"))
        os.makedirs(os.path.join(indir, "val", "song"))
        os.makedirs(os.path.join(indir, "val", "hum"))
    
    count = 0
    for id in val_set:
        for song_filename, hum_filename in unique_music[id]:
            # print(f'Move file {os.path.join(indir, "train", "song", song_filename + ".npy")} to val set')
            song_path = os.path.join(indir, "train", "song", song_filename + ".npy")
            hum_path = os.path.join(indir, "train", "hum", hum_filename + ".npy")

            if not os.path.isfile(song_path) or not os.path.isfile(hum_path):
                print(f'Not found: song={song_filename}.npy or hum={hum_filename}.npy')
                continue

            os.rename(song_path, os.path.join(indir, "val", "song", song_filename + ".npy"))
            os.rename(hum_path, os.path.join(indir, "val", "hum", hum_filename + ".npy"))
            count += 1
    print(f"Count: {count} files")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=str, required=False, 
                        default="config/preprocess.yaml",
                        help="path to preprocess.yaml")
    parser.add_argument("--indir", type=str, required=False, help="path to input/outdir")
    parser.add_argument("--val_size", type=int, required=False, default=181, help="val size")
    args = parser.parse_args()

    config = yaml.load(open(args.config, "r"), Loader=yaml.FullLoader)
    if args.indir is not None:
        config["path"]["preprocessed_path"] = args.indir
    if args.val_size is not None:
        config["preprocessing"]["val_size"] = args.val_size

    main(config)