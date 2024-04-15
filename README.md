# Music Stream Analysis
Analysis of music streaming services. Scraping data from Spotify and Youtube. 

## Database Schema

* Artists: Store artist information (artist_id (primary key), name, popularity, followers).
* Tracks: Store information about songs (song_id (primary key), artist_id (foreign key), name, popularity, album_id (foreign key), release date).
* Albums: Store album information (e.g., album_id (primary key), artist_id (foreign key), name, release date).
* Markets: Store available markets (e.g., market_id (primary key), country codes, name).
* Genres: Store possible genres (e.g., genre seeds).
* ArtistMarketPerformance: Track performance metrics of artists in different markets (e.g., artist_id (primary key = artists.artist_id), market_id (foreign key), stream counts, follower count change).
* EventImpact: Analyze impacts of events on streaming (e.g., event_id (primary key), artist_id (foreign key), event type [concert, album release], event date, location, market_id (foreign key), before and after metrics).

## Integrating External Data (Optional)
Consider integrating external data sources to enrich your analysis:

Concert dates and locations from platforms like Bandsintown or Songkick.
Social media metrics from Twitter or Instagram APIs to analyze the impact of social media presence on streaming numbers.