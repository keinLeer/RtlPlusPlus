# RtlPlusPlus
## What is this about?
- Scraping RTL+ (RTL media library) for episodes
- Collecting episodes in a neo4j database
- Searching the database for videos

## How to use
- Start the main function of the Main object
- TODO: Dynamic interface (CLI, GUI)

## Why?
- Practice
  - scala
  - scala libraries
    - akka
      - stream
      - http
    - circe
  - neo4j
    - as a content database
    - as a semantic web
  - NLP and searches
- RTL+ is lacking basic functionality for a media library
  - searching for episodes by name
  - searching through episodes by season/episode
  - browsing series in order of season/episode
  - finding series/seasons/episodes using "tags", "themes", etc.