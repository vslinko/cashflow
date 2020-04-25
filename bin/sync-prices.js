#!/usr/bin/env node
require("dotenv").config();
const pg = require("pg");
const fs = require("fs");
const chalk = require("chalk");
const fork = require("child_process").fork;
const OpenAPI = require("@tinkoff/invest-openapi-js-sdk");
const { DateTime } = require("luxon");
const stringifyCsv = require("csv-stringify/lib/sync");
const copyFrom = require("pg-copy-streams").from;
const argv = require("yargs").option("refresh", {
  boolean: true,
  default: true
}).argv;

const apiURL = process.env.TINKOFF_API_URL;
const socketURL = process.env.TINKOFF_SOCKET_URL;
const secretToken = process.env.TINKOFF_TOKEN;

const tempFile = process.cwd() + "/temp.csv";

async function getCandles(api, ticker, from) {
  const now = DateTime.local();

  let resultCandles = [];

  const { figi } = await api.searchOne({ ticker });

  while (from < now) {
    const to = DateTime.min(from.plus({ year: 1 }), now);

    const { candles } = await api.candlesGet({
      from: from.toISO(),
      to: to.toISO(),
      figi,
      interval: "day"
    });

    resultCandles = resultCandles.concat(candles);
    from = to.plus({ days: 1 });
  }

  return resultCandles.map(r => ({
    ticker,
    o: r.o,
    c: r.c,
    h: r.h,
    l: r.l,
    v: r.v,
    time: DateTime.fromISO(r.time).toISO(),
    interval: r.interval
  }));
}

async function main() {
  try {
    process.stdout.write(chalk.yellow("Syncing prices\n"));

    const api = new OpenAPI({ apiURL, secretToken, socketURL });

    const conn = new pg.Client({
      host: process.env.PSQL_HOST,
      port: process.env.PSQL_PORT,
      user: process.env.PSQL_USER,
      password: process.env.PSQL_PASS,
      database: process.env.PSQL_DB
    });
    await conn.connect();

    const tickers = (
      await conn.query(`
        select
            ticker,
            min(date) as started_at
        from
            portfolio_operations
        where
            ticker != 'MONEY'
        group by
            ticker
      `)
    ).rows.map(row => [row.ticker, DateTime.fromJSDate(row.started_at)]);

    tickers.push(["USD000UTSTOM", DateTime.fromISO("2016-01-01")]);

    await conn.query(`truncate portfolio_prices`);

    for (const [ticker, startedAt] of tickers) {
      try {
        process.stdout.write(`\t${ticker} `);
        const candles = await getCandles(api, ticker, startedAt);

        const csv = stringifyCsv(candles, {
          header: true
        });
        fs.writeFileSync(tempFile, csv);

        await new Promise((resolve, reject) => {
          const stream = conn.query(
            copyFrom(`copy portfolio_prices from stdin with csv header;`)
          );
          const fileStream = fs.createReadStream(tempFile);
          fileStream.on("error", reject);
          stream.on("error", reject);
          stream.on("end", resolve);
          fileStream.pipe(stream);
        });
        process.stdout.write(chalk.green("OK\n"));
      } catch (err) {
        process.stdout.write(chalk.red("ERROR\n"));
        throw err;
      }
    }

    if (fs.existsSync(tempFile)) {
      fs.unlinkSync(tempFile);
    }

    if (argv.refresh) {
      await new Promise((resolve, reject) => {
        const p = fork(
          __dirname + "/refresh-materialized-views.js",
          ["--views", "portfolio_performance"],
          {
            stdio: "inherit"
          }
        );
        p.on("exit", code => {
          if (code > 0) {
            reject(new Error(`Exited with code ${code}`));
          } else {
            resolve();
          }
        });
      });
    }

    conn.end();
  } catch (err) {
    console.error(err);
    process.exit(1);
  }
}

main();
