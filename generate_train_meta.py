#!/usr/bin/env python3
"""
Script to generate train_meta.csv by properly mapping song files to hum files
based on group_id and fragment_id from the CHAD dataset.
"""

import os
import csv
from collections import defaultdict

def parse_filename(filename, file_type='song'):
    """
    Parse filename to extract group_id and fragment_id.

    Song format: {group_id}_{fragment_id}_{id}.mp3
    Hum format:  hum_{group_id}_{fragment_id}_{id}.wav

    Returns: (group_id, fragment_id, full_filename)
    """
    basename = filename.replace('.mp3', '').replace('.wav', '')

    if file_type == 'hum':
        # Remove 'hum_' prefix
        basename = basename.replace('hum_', '')

    parts = basename.split('_')
    if len(parts) >= 2:
        group_id = parts[0]
        fragment_id = parts[1]
        music_id = f"{group_id}_{fragment_id}"
        return music_id, filename
    else:
        return None, filename

def generate_train_meta(chad_dir, output_csv):
    """
    Generate train_meta.csv by mapping songs to hums based on music_id.
    """
    song_dir = os.path.join(chad_dir, 'song')
    hum_dir = os.path.join(chad_dir, 'hum')

    # Organize songs by music_id
    songs_by_id = defaultdict(list)
    for song_file in os.listdir(song_dir):
        if song_file.endswith('.mp3'):
            music_id, filename = parse_filename(song_file, 'song')
            if music_id:
                songs_by_id[music_id].append(filename)

    # Organize hums by music_id
    hums_by_id = defaultdict(list)
    for hum_file in os.listdir(hum_dir):
        if hum_file.endswith('.wav'):
            music_id, filename = parse_filename(hum_file, 'hum')
            if music_id:
                hums_by_id[music_id].append(filename)

    # Generate mappings
    mappings = []

    for music_id in sorted(songs_by_id.keys()):
        songs = songs_by_id[music_id]
        hums = hums_by_id.get(music_id, [])

        if not hums:
            print(f"Warning: No hums found for music_id={music_id}")
            continue

        # For each song, map to all hums with the same music_id
        for song in songs:
            for hum in hums:
                mappings.append({
                    'music_id': music_id,
                    'song_path': f'song/{song}',
                    'hum_path': f'hum/{hum}'
                })

    # Write CSV
    with open(output_csv, 'w', newline='') as csvfile:
        fieldnames = ['music_id', 'song_path', 'hum_path']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
        for mapping in mappings:
            writer.writerow(mapping)

    # Print statistics
    print(f"\nGenerated {output_csv}")
    print(f"Total mappings: {len(mappings)}")
    print(f"Unique music_ids: {len(songs_by_id)}")
    print(f"Total songs: {sum(len(songs) for songs in songs_by_id.values())}")
    print(f"Total hums: {sum(len(hums) for hums in hums_by_id.values())}")

    # Show sample
    print("\nSample mappings (first 5):")
    for i, mapping in enumerate(mappings[:5]):
        print(f"  {mapping['music_id']}: {mapping['song_path']} -> {mapping['hum_path']}")

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Generate train_meta.csv from CHAD dataset')
    parser.add_argument('--chad_dir', type=str, default='/home/thinv/hum2song/chad',
                        help='Path to CHAD directory')
    parser.add_argument('--output', type=str, default=None,
                        help='Output CSV path (default: chad/train_meta.csv)')

    args = parser.parse_args()

    output_csv = args.output or os.path.join(args.chad_dir, 'train_meta.csv')

    generate_train_meta(args.chad_dir, output_csv)
