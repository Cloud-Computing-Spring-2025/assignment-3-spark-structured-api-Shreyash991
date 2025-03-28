# Music Streaming Data Analysis with Spark

This project analyzes user listening behavior and music trends using Spark Structured APIs on data from a fictional music streaming platform.

## Prerequisites

Before starting the assignment, ensure you have the following software installed and properly configured on your machine:

**Python 3.x**:
- Download and Install Python
- Verify installation:
```
python --version
```

**PySpark**:
- Install using pip:
```
pip install pyspark
```

**Apache Spark**:
- Ensure Spark is installed. You can download it from the [Apache Spark Downloads](https://spark.apache.org/downloads.html) page.
- Verify installation by running:
```
spark-submit --version
```

**Docker & Docker Compose** (Optional):
- If you prefer using Docker for setting up Spark, ensure Docker and Docker Compose are installed.
- [Docker Installation Guide](https://docs.docker.com/get-docker/)
- [Docker Compose Installation Guide](https://docs.docker.com/compose/install/)

## Overview

The analysis covers various aspects of user behavior with music streaming data:

- User genre preferences
- Song popularity metrics
- Average listening times
- Personalized song recommendations
- Genre loyalty analysis
- Nighttime listening patterns

## Datasets

Two synthetic datasets were created for this analysis:

1. **listening_logs.csv**: Contains user listening activity logs with:
   - user_id: Unique user identifier
   - song_id: Unique song identifier
   - timestamp: Date and time when the song was played
   - duration_sec: Duration in seconds for which the song was played

2. **songs_metadata.csv**: Contains song metadata with:
   - song_id: Unique song identifier
   - title: Song title
   - artist: Artist name
   - genre: Song genre (Pop, Rock, Jazz, etc.)
   - mood: Mood category (Happy, Sad, Energetic, Chill)

## Analysis Tasks

### 1. User Favorite Genres
**Objective**: Identify the most listened-to genre for each user by counting how many times they played songs in each genre.

**Output**:
```
+-------+------+----------+
|user_id| genre|play_count|
+-------+------+----------+
|      1|   Pop|       135|
|      2| Metal|       128|
|      3|  Rock|       112|
...
```

---

### 2. Average Listen Time Per Song
**Objective**: Compute the average duration (in seconds) for each song based on user play history.

**Output**:
```
+-------+---------------+------------------+----------------+
|song_id|          title|            artist|avg_duration_sec|
+-------+---------------+------------------+----------------+
|     42|       Song 42 |Rock Revolution   |          175.32|
|     18|Happy Song 18  |Jazz Ensemble     |          168.45|
...
```

---

### 3. Top 10 Most Played Songs This Week
**Objective**: Determine which songs were played the most in the current week and return the top 10 based on play count.

**Output**:
```
+-------+------------+---------------+-----+
|song_id|       title|         artist|plays|
+-------+------------+---------------+-----+
|     23|Sad Song 3  |Electronic Waves|   42|
|      7|Happy Song 7|Pop Sensation   |   38|
...
```

---

### 4. "Happy" Song Recommendations
**Objective**: Recommend "Happy" songs to users who primarily listen to "Sad" songs.

**Output**:
```
+-------+-------+------------------+------------------+
|user_id|song_id|             title|            artist|
+-------+-------+------------------+------------------+
|     13|     12|     Happy Song 12|Classical Symphony|
|     13|     19|     Happy Song 19|          R&B Soul|
|      6|     18|     Happy Song 18|     Jazz Ensemble|
|      6|     19|     Happy Song 19|          R&B Soul|
|      3|      1|      Happy Song 1|    Electric Pulse|
|      3|     13|     Happy Song 13|    Hip-Hop Heroes|
|     19|      5|      Happy Song 5|         Folk Tales|
|     19|     12|     Happy Song 12|Classical Symphony|
|      4|      7|      Happy Song 7|     Pop Sensation|
|      4|     12|     Happy Song 12|Classical Symphony|
...
```

---

### 5. Genre Loyalty Scores
**Objective**: Calculate the proportion of each user's plays that belong to their most-listened genre and output users with a loyalty score above 0.6.

**Output**:
```
+-------+-------------------+
|user_id|      loyalty_score|
+-------+-------------------+
|      5| 0.9263565891472868|
|      8| 0.9019607843137255|
|      6| 0.8966942148760331|
|      7|              0.884|
...
```

---

### 6. Night Owl Users
**Objective**: Extract users who frequently listen to music between 12 AM and 5 AM based on their listening timestamps.

**Output**:
```
+-------+------------+
|user_id|night_listens|
+-------+------------+
|     12|          56|
|      3|          43|
|      7|          29|
...
```

---

## Running the Code

### Generate the datasets:
```
python generate_data.py
```

### Run the Spark analysis:
```
spark-submit music_analysis.py
```

## Output Structure

The results of the analysis are saved in the following directory structure:

```
output/
├── user_favorite_genres/
├── avg_listen_time_per_song/
├── top_songs_this_week/
├── happy_recommendations/
├── genre_loyalty_scores/
├── night_owl_users/
└── enriched_logs/
```

## Errors and Resolutions

### 1. Empty DataFrame Handling
**Error**: Union operation failed when recommendations list was empty.
**Solution**: Added conditional checks to handle empty DataFrames and create schema-compatible empty DataFrames when needed.

### 2. Threshold Adjustments
**Error**: Initially no users met the high loyalty threshold (0.8) in randomly generated data.
**Solution**: Modified the data generation to include users with high genre loyalty and adjusted the threshold to 0.6.
