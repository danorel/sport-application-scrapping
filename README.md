# sport-application-scrapping

sport-application-scrapping is a Python project for dealing with scrapping of Sport data.

## Installation & Contributing

Use the [DEVELOPER.md](./DEVELOPER.md) guide to run or contribute to the project.

## Usage

0. Setup processing infrastructure:
```zsh
docker-compose up -d
```

Run in *four* terminal windows following scripts:

1. Scrap the data from OpenStreetMap:
```zsh
python -m jobs.osm_scrap --html_pages=5
```

2. Extract the data from OpenStreetMap:
```zsh
python -m jobs.osm_extract
```

3. Process and normalize the data from OpenStreetMap:
```zsh
python -m jobs.osm_transform
```

4. Load the processed data from OpenStreetMap into Neo4j:
```zsh
python -m jobs.osm_load
```

## License

[MIT](./LICENSE)
