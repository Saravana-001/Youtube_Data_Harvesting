# YouTube Data Harvesting and Warehousing Application

This application allows users to collect, store, and analyze YouTube channel data. It is built using Python, Streamlit, MongoDB, PostgreSQL, and the YouTube Data API v3. The application supports data harvesting from YouTube channels, data warehousing in MongoDB and PostgreSQL, and interactive querying through a user-friendly interface.

## Features

- **Data Collection**: Collect detailed information about YouTube channels, playlists, videos, and comments using the YouTube Data API v3.
- **Data Storage**: Store the collected data in MongoDB for easy access and migration.
- **Data Migration**: Migrate the stored data from MongoDB to PostgreSQL for advanced querying and analysis.
- **Interactive Queries**: Execute predefined queries to analyze data and visualize insights using Streamlit.

## Prerequisites

Before running the application, ensure you have the following installed:

- Python 3.8+
- MongoDB
- PostgreSQL
- Streamlit

## Installation

1. Clone the repository:
    ```bash
    git clone https://github.com/yourusername/youtube-data-harvesting.git
    cd youtube-data-harvesting
    ```

2. Install the required Python packages:
    ```bash
    pip install -r requirements.txt
    ```

3. Set up MongoDB:
   - Create a MongoDB Atlas account or set up a local MongoDB instance.
   - Create a database named `Youtube_data`.

4. Set up PostgreSQL:
   - Install PostgreSQL and create a database named `youtube_data`.
   - Ensure you have a user with appropriate privileges to access the database.

5. Obtain a YouTube Data API v3 key:
   - Go to the [Google Developers Console](https://console.developers.google.com/).
   - Create a project and enable the YouTube Data API v3.
   - Generate an API key and replace the placeholder in the code.

## Usage

1. **Run the Streamlit application:**
    ```bash
    streamlit run app.py
    ```

2. **Navigate to the Streamlit interface:**
   - Open your web browser and go to `http://localhost:8501`.

3. **Collect YouTube Data:**
   - Enter the YouTube channel ID in the input box under "Data Collection."
   - Click "Collect and Store Data" to fetch channel data and store it in MongoDB.

4. **Migrate Data to SQL:**
   - Click "Migrate to SQL" to transfer the stored data from MongoDB to PostgreSQL.

5. **View Data and Run Queries:**
   - Use the "Data Visualization & Queries" section to view the stored data.
   - Select predefined queries to explore the data and gain insights.

## Project Structure

- **app.py**: The main Streamlit application file that contains the logic for data collection, migration, and querying.
- **requirements.txt**: List of required Python packages.
- **README.md**: This file.

## Configuration

- **API Key**: Update the `Api_Id` variable in the `Api_connect` function with your YouTube Data API v3 key.
- **MongoDB Connection**: Update the MongoDB connection string in `pymongo.MongoClient()` to match your MongoDB instance.
- **PostgreSQL Connection**: Update the PostgreSQL connection parameters in the `psycopg2.connect()` function to match your PostgreSQL instance.

## Queries Included

1. All the videos and the channel names.
2. Channels with the most number of videos.
3. 10 most viewed videos.
4. Comments on each video.
5. Videos with the highest likes.
6. Likes of all videos.
7. Views of each channel.
8. Videos published in the year 2022.
9. Average duration of all videos in each channel.
10. Videos with the highest number of comments.

## Contributing

Contributions are welcome! Please fork the repository and submit a pull request for any improvements or new features.

## License

This project is licensed under the MIT License. See the `LICENSE` file for more details.
