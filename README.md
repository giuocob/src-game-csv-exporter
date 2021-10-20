# src-game-csv-exporter

Simple tool that uses the Speedrun.com REST API to dump CSVs of each leaderboard for a given game. This was only tested on `ladx` and probably doesn't work correctly. for some other game/leaderboard configurations.

## Usage

```
npm install
node src-csv-exporter.js -g ladx -o csv-output
```

`node src-csv-exporter.js --help` displays additional command line options.
