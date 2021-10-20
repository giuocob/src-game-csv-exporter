import fs from 'fs';
import path from 'path';
import got from 'got';
import { Command } from 'commander/esm.mjs';
import pasync from 'pasync';
import mkdirp from 'mkdirp';
import zstreams from 'zstreams';
import CsvWrite from 'zstreams-csv-write';

const program = new Command();
program
	.requiredOption('-g, --game <game>', 'SRC abbreviation for the game to be exported.')
	.requiredOption('-o, --output-dir <outputDir>', 'Directory in which to place output files.')
	.option('-v, --breakout-variables', 'If true, leaderboards will be subdivided into each allowed combination of category variables.')
	.option('-d, --debug', 'Print verbose debug messages while running.')
	.option('-a, --include-unverified-runs', 'Includes runs of any status (by default, only verified runs are dumped).')
	.option('-h, --include-run-history', 'Includes all obsolete runs in the dump.')
	.option('-f, --output-format <format>', 'Specifies the format for the output data dump. Allowed values are "csv" and "json" (default is "csv").')
	.parse(process.argv);

function debugLog(str) {
	if (!program.opts().debug) return;
	console.log(str);
}

const SPEEDRUN_URL_BASE = 'https://www.speedrun.com/api/v1/';
// (very lazy) throttling: Speedrun.com throttles each IP to 100 requests per minute. Requests will not be sent from this function more than every second.
let lastRequestTimestamp = 0;
const REQUEST_TIMESTAMP_INTERVAL = 1000;

async function speedrunApiRequest(methodPath, qs) {
	let timestampDiff = Date.now() - lastRequestTimestamp;
	if (timestampDiff < REQUEST_TIMESTAMP_INTERVAL) {
		await pasync.setTimeout(REQUEST_TIMESTAMP_INTERVAL - timestampDiff);
	}
	lastRequestTimestamp = Date.now();

	let url = `${SPEEDRUN_URL_BASE}${methodPath}`;
	debugLog(`Placing speedrun.com API request to ${url}, query parameters ${JSON.stringify(qs || {})}`);
	let response;
	await pasync.retry(3, async () => {
		try {
			response = await got(url, { searchParams: qs, responseType: 'json' });
		} catch (err) {
			debugLog(err);
			await pasync.setTimeout(10000);
			throw err;
		}
	})
	if (!response.body || !response.body.data) throw new Error('Unexpected response format');
	return response.body.data;
}

const PAGINATE_PAGE_SIZE = 100;
async function paginateSpeedrunApiRequest(methodPath, qs, task) {
	if (!qs) qs = {};
	qs.offset = 0;
	qs.max = PAGINATE_PAGE_SIZE;
	while (true) {
		let pageData = await speedrunApiRequest(methodPath, qs);
		for (let dataObj of pageData) {
			await task(dataObj);
		}
		if (pageData.length < PAGINATE_PAGE_SIZE) {
			break;
		} else {
			qs.offset += PAGINATE_PAGE_SIZE;
		}
	}
}


async function run() {
	let popts = program.opts();
	let gameAbbrev = popts.game.toLowerCase();
	let categories = {};
	let leaderboards = {};
	let outputDir;
	if (path.isAbsolute(popts.outputDir)) {
		outputDir = popts.outputDir;
	} else {
		outputDir = path.resolve(process.cwd(), popts.outputDir);
	}
	mkdirp.sync(outputDir);
	let outputFormat = popts.outputFormat || 'csv';
	if (outputFormat !== 'csv' && outputFormat !== 'json') throw new Error('Invalid outputFormat');


	let platformMap = {};
	async function getPlatform(platformId) {
		if (!platformId) return;
		if (platformMap[platformId]) return platformMap[platformId];
		let response = await speedrunApiRequest(`platforms/${platformId}`);
		if (response && response.name) {
			platformMap[platformId] = response.name;
			return response.name;
		}
	}

	let userMap = {};
	async function getUsername(userId) {
		if (!userId) return;
		if (userMap[userId]) return userMap[userId];
		let response = await speedrunApiRequest(`users/${userId}`);
		if (response && response.names && response.names.international) {
			userMap[userId] = response.names.international;
			return response.names.international;
		}
	}

	async function processRun(runResponseObj) {
		if (!runResponseObj || !runResponseObj) return null;
		let runObj = {
			id: runResponseObj.id,
			comment: runResponseObj.comment,
			submitTime: runResponseObj.submitted,
			status: runResponseObj.status.status,
			variables: runResponseObj.values || {}
		};
		if (
			!runObj.status ||
			(!popts.includeUnverifiedRuns && (runObj.status !== 'verified'))
		) {
			return null;
		}

		if (runResponseObj.times && runResponseObj.times.primary_t) {
			let unresolvedTime = Math.floor(runResponseObj.times.primary_t);
			let seconds, hours, minutes;
			seconds = unresolvedTime % 60;
			unresolvedTime -= seconds;
			minutes = unresolvedTime % 3600;
			unresolvedTime -= minutes;
			minutes /= 60;
			hours = unresolvedTime / 3600;
			runObj.time = `${('' + hours).padStart(2, '0')}:${('' + minutes).padStart(2, '0')}:${('' + seconds).padStart(2, '0')}`;
			runObj.timeSeconds = runResponseObj.times.primary_t;
		}
		if (!runObj.timeSeconds) return null;

		if (runObj.status === 'verified') {
			runObj.verifyTime = runResponseObj.status['verify-date'];
			runObj.verifier = await getUsername(runResponseObj.status.examiner);
		}
		if (
			runResponseObj.videos &&
			runResponseObj.videos.links &&
			runResponseObj.videos.links[0]
		) {
			runObj.videos = runResponseObj.videos.links
				.map((obj) => obj.uri)
				.filter((str) => !!str);
		}
		if (runResponseObj.system && runResponseObj.system.platform) {
			runObj.platform = await getPlatform(runResponseObj.system.platform);
		}
		if (
			runResponseObj.players &&
			(runResponseObj.players.length === 1)
		) {
			let playerObj = runResponseObj.players[0];
			if (playerObj.rel === 'guest') {
				runObj.player = {
					rel: 'guest',
					id: playerObj.name,
					name: playerObj.name
				};
			} else if (playerObj.rel === 'user') {
				runObj.player = {
					rel: 'user',
					id: playerObj.id,
					name: await getUsername(playerObj.id)
				};
			}
		}

		return runObj;
	}


	let gameResponse = await speedrunApiRequest('games', { abbreviation: gameAbbrev });
	if (!gameResponse[0]) throw new Error('Game not found');
	let gameId = gameResponse[0].id;
	
	let catResponse = await speedrunApiRequest(`games/${gameId}/categories`);
	for (let catResponseObj of catResponse) {
		let catObj = {
			id: catResponseObj.id,
			name: catResponseObj.name,
			rules: catResponseObj.rules,
			variables: [],
			runs: []
		};
		let varResponse = await speedrunApiRequest(`categories/${catObj.id}/variables`);
		for (let varResponseObj of varResponse) {
			let varObj = {
				id: varResponseObj.id,
				name: varResponseObj.name,
				values: {}
			};
			let valuesMap = varResponseObj.values && varResponseObj.values.values;
			for (let valueKey in valuesMap || {}) {
				varObj.values[valueKey] = {
					label: valuesMap[valueKey].label,
					rules: valuesMap[valueKey].rules
				};
			}
			catObj.variables.push(varObj);
		}

		await paginateSpeedrunApiRequest(`runs`, { category: catObj.id, orderby: 'date', direction: 'asc' }, async (runResponseObj) => {
			let runObj = await processRun(runResponseObj);
			if (runObj) catObj.runs.push(runObj);
		});
		categories[catObj.id] = catObj;
	}

	// Split each category by variable as needed
	for (let catId in categories) {
		let catObj = categories[catId];
		if (popts.breakoutVariables) {
			for (let runObj of catObj.runs) {
				let usedVars = {};
				let orderedVars = [];
				let uncategorized = false;
				for (let catVariable of catObj.variables) {
					if (!runObj.variables[catVariable.id]) {
						uncategorized = true;
						break;
					}
					if (!catVariable.values[runObj.variables[catVariable.id]]) {
						uncategorized = true;
						break;
					}
					usedVars[catVariable.id] = runObj.variables[catVariable.id];
					orderedVars.push({ key: catVariable.id, value: runObj.variables[catVariable.id] });
				}
				
				let lbId, lbName;
				if (uncategorized) {
					lbId = `${catId}$UNCATEGORIZED`;
					lbName = `${catObj.name} - Uncategorized`;
				} else {
					lbId = `${catId}`;
					lbName = `${catObj.name}`;
					for (let i = 0; i < orderedVars.length; i++) {
						lbId += '$';
						if (i === 0) {
							lbName += ' - ';
						} else {
							lbName += ', ';
						}
						lbId += `${orderedVars[i].key}^${orderedVars[i].value}`;
						lbName += catObj.variables[i].values[orderedVars[i].value].label;
					}
				}

				if (!leaderboards[lbId]) {
					leaderboards[lbId] = {
						id: lbId,
						categoryId: catId,
						variables: usedVars,
						name: lbName,
						runs: []
					};
				}
				leaderboards[lbId].runs.push(runObj);
			}
		} else {
			let lbObj = {
				id: catId,
				categoryId: catId,
				variables: {},
				name: catObj.name,
				runs: catObj.runs
			};
			leaderboards[lbObj.id] = lbObj;
		}

		delete catObj.runs;
	}

	// For each leaderboard, remove obsolete runs and sort by time
	for (let lbId in leaderboards) {
		let lbObj = leaderboards[lbId];
		let runsByUserId = {};
		for (let run of lbObj.runs) {
			if (!run.timeSeconds || !run.player || !run.player.id) continue;
			if (!runsByUserId[run.player.id]) runsByUserId[run.player.id] = [];
			runsByUserId[run.player.id].push(run);
		}
		let lbEntries = [];
		for (let userId in runsByUserId) {
			let userRuns = runsByUserId[userId].sort((a, b) => a.timeSeconds - b.timeSeconds);
			if (userRuns.length === 0) continue;
			let entry = userRuns[0];
			if (popts.includeRunHistory) {
				if (userRuns.length > 1) {
					entry.obsoleteRuns = userRuns.slice(1);
				} else {
					entry.obsoleteRuns = [];
				}
			}
			lbEntries.push(entry);
		}
		lbEntries.sort((a, b) => a.timeSeconds - b.timeSeconds);
		for (let i = 0; i < lbEntries.length; i++) {
			if (i === 0) {
				lbEntries[i].rank = 1;
			} else if (lbEntries[i].timeSeconds === lbEntries[i - 1].timeSeconds) {
				lbEntries[i].rank = lbEntries[i - 1].rank;
			} else {
				lbEntries[i].rank = i + 1;
			}
		}
		lbObj.runs = lbEntries;
	}


	if (outputFormat === 'csv') {

		function makeCsvLine(run) {
			return {
				rank: run.rank,
				player: run.player && run.player.name,
				time: run.time,
				platform: run.platform,
				videos: run.videos && run.videos.join(', '),
				submitTime: run.submitTime,
				status: run.status,
				verifyTime: run.verifyTime,
				verifier: run.verifier,
				comment: run.comment,
				variables: (Object.keys(run.variables).length > 0) ? JSON.stringify(run.variables) : ''
			};
		}

		for (let lbId in leaderboards) {
			let lbObj = leaderboards[lbId];
			if (!lbObj.runs || lbObj.runs.length === 0) continue;
			let filename = path.resolve(outputDir, `${gameAbbrev}_${lbObj.name.toLowerCase().replace(/[\s\/\_-]+/g, '_')}.csv`);
			let csvLines = [];
			for (let run of lbObj.runs) {
				csvLines.push(makeCsvLine(run));
				if (run.obsoleteRuns) {
					for (let obsoleteRun of run.obsoleteRuns) {
						csvLines.push(makeCsvLine(obsoleteRun));
					}
				}
			}
			await zstreams.fromArray(csvLines)
				.pipe(new CsvWrite([
					'rank', 'player', 'time', 'platform', 'videos', 'submitTime', 'status', 'verifyTime', 'verifier', 'comment', 'variables'
				]))
				.intoFile(filename);

		}
	} else if (outputFormat === 'json') {
		let outputObj = {
			game: {
				id: gameId,
				abbrev: gameAbbrev
			},
			categories: categories,
			leaderboards: leaderboards
		};
		fs.writeFileSync(path.resolve(outputDir, `${gameAbbrev}.json`), JSON.stringify(outputObj, null, 4) + '\n');
	}
}

run()
	.then(() => console.log('OK'));
