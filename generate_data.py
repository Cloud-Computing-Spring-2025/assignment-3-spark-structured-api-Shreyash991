import csv
import os
import random
from datetime import datetime, timedelta

# Create directories if they don't exist
os.makedirs("input", exist_ok=True)
os.makedirs("output", exist_ok=True)
for folder in ["user_favorite_genres", "avg_listen_time_per_song", "top_songs_this_week", 
               "happy_recommendations", "genre_loyalty_scores", "night_owl_users", "enriched_logs"]:
    os.makedirs(f"output/{folder}", exist_ok=True)

# Sample data for songs_metadata.csv
genres = ["Pop", "Rock", "Jazz", "Hip-Hop", "Classical", "Electronic", "R&B", "Country", "Folk", "Metal"]
moods = ["Happy", "Sad", "Energetic", "Chill", "Romantic", "Angry", "Melancholic", "Peaceful"]
artists = [
    "The Melody Makers", "Electric Pulse", "Acoustic Dreams", "Urban Beats", "Classical Symphony",
    "Jazz Ensemble", "Country Roads", "Folk Tales", "Metal Madness", "Pop Sensation",
    "Rock Revolution", "Smooth Jazz", "Hip-Hop Heroes", "Electronic Waves", "R&B Soul"
]

# Generate songs_metadata.csv
songs_data = []
# First, ensure we have at least 20 Happy songs and 20 Sad songs
for song_id in range(1, 21):
    title = f"Happy Song {song_id}"
    artist = random.choice(artists)
    genre = random.choice(genres)
    
    song = {
        "song_id": song_id,
        "title": title,
        "artist": artist,
        "genre": genre,
        "mood": "Happy"
    }
    songs_data.append(song)

for song_id in range(21, 41):
    title = f"Sad Song {song_id-20}"
    artist = random.choice(artists)
    genre = random.choice(genres)
    
    song = {
        "song_id": song_id,
        "title": title,
        "artist": artist,
        "genre": genre,
        "mood": "Sad"
    }
    songs_data.append(song)

# Add the rest of the songs with random moods
for song_id in range(41, 101):
    title = f"Song {song_id}"
    artist = random.choice(artists)
    genre = random.choice(genres)
    mood = random.choice(moods)
    
    song = {
        "song_id": song_id,
        "title": title,
        "artist": artist,
        "genre": genre,
        "mood": mood
    }
    songs_data.append(song)

with open("input/songs_metadata.csv", mode="w", newline="", encoding="utf-8") as file:
    writer = csv.DictWriter(file, fieldnames=songs_data[0].keys())
    writer.writeheader()
    writer.writerows(songs_data)

# Generate listening_logs.csv
num_users = 20  # Number of users
num_logs = 5000  # Number of listening logs
logs_data = []

# Generate a base time that's "this week"
now = datetime.now()
start_of_week = now - timedelta(days=now.weekday())
end_time = now
start_time = now - timedelta(days=30)  # Data from the last 30 days

# Create 4 dedicated "sad listeners" who primarily listen to sad songs
sad_listeners = [1, 2, 3, 4]

# Create 4 users with high genre loyalty (users 5-8)
loyal_users = {
    5: "Pop",
    6: "Rock",
    7: "Jazz",
    8: "Hip-Hop"
}

for log_id in range(1, num_logs + 1):
    user_id = random.randint(1, num_users)
    
    # For sad listeners, make them listen to sad songs 70% of the time
    if user_id in sad_listeners and random.random() < 0.7:
        # Select a random sad song (IDs 21-40)
        song_id = random.randint(21, 40)
    # For users with high genre loyalty, make them listen to their preferred genre 90% of the time
    elif user_id in loyal_users and random.random() < 0.9:
        preferred_genre = loyal_users[user_id]
        # Find songs with this genre
        matching_songs = [song["song_id"] for song in songs_data if song["genre"] == preferred_genre]
        if matching_songs:
            song_id = random.choice(matching_songs)
        else:
            song_id = random.randint(1, 100)
    else:
        # Regular random song selection for other users
        song_id = random.randint(1, 100)
    
    # Random timestamp within the last 30 days
    timestamp = start_time + (end_time - start_time) * random.random()
    timestamp_str = timestamp.strftime("%Y-%m-%d %H:%M:%S")
    
    # Random duration between 30 seconds and 5 minutes (300 seconds)
    duration_sec = random.randint(30, 300)
    
    log = {
        "user_id": user_id,
        "song_id": song_id,
        "timestamp": timestamp_str,
        "duration_sec": duration_sec
    }
    logs_data.append(log)

with open("input/listening_logs.csv", mode="w", newline="", encoding="utf-8") as file:
    writer = csv.DictWriter(file, fieldnames=logs_data[0].keys())
    writer.writeheader()
    writer.writerows(logs_data)

print("✅ Dataset generation complete: 'songs_metadata.csv' and 'listening_logs.csv' created in /input/")
print("📊 Dataset includes:")
print(" - 20 Happy songs (IDs 1-20)")
print(" - 20 Sad songs (IDs 21-40)")
print(" - 60 songs with random moods (IDs 41-100)")
print(" - 4 users (IDs 1-4) who listen primarily to sad songs")
print(" - 4 users (IDs 5-8) with high genre loyalty")
