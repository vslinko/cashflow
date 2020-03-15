#!/usr/bin/env node
require("dotenv").config();
const pg = require("pg");
const fs = require("fs");
const fetch = require("node-fetch");
const chalk = require("chalk");
const fork = require("child_process").fork;
const { DateTime } = require("luxon");
const stringifyCsv = require("csv-stringify/lib/sync");
const copyFrom = require("pg-copy-streams").from;
const argv = require("yargs").option("refresh", {
  boolean: true,
  default: true
}).argv;

const tempFile = process.cwd() + "/temp.csv";

async function login() {
  const body = `email=${encodeURIComponent(
    process.env.BT_EMAIL
  )}&password=${encodeURIComponent(process.env.BT_PASSWORD)}&login=`;
  const res = await fetch("https://blackterminal.ru/login", {
    method: "POST",
    redirect: "manual",
    headers: {
      "Content-Type": "application/x-www-form-urlencoded"
    },
    body
  });

  const h = res.headers
    .raw()
    ["set-cookie"].map(r => /([^=;]+)=([^=;]+)/.exec(r))
    .reduce((acc, r) => {
      acc[r[1].trim()] = r[2].trim();
      return acc;
    }, {});

  return h;
}

async function get(cookies) {
  const res = await fetch(
    `https://blackterminal.ru/tools/ajax-portfolio-export.php?id=${process.env.BT_PORTFOLIO_ID}&service=bt_json`,
    {
      headers: {
        Cookie: Object.entries(cookies)
          .map(([k, v]) => `${k}=${v}`)
          .join(";")
      }
    }
  );
  return await res.json();
}

async function main() {
  try {
    process.stdout.write(chalk.yellow("Syncing portfolio\n"));

    const conn = new pg.Client({
      host: process.env.PSQL_HOST,
      port: process.env.PSQL_PORT,
      user: process.env.PSQL_USER,
      password: process.env.PSQL_PASS,
      database: process.env.PSQL_DB
    });
    await conn.connect();

    try {
      process.stdout.write("\tsyncing ");
      const cookies = await login();
      const result = await get(cookies);

      const operations = Object.values(result.transactions).map(r => ({
        id: r.id,
        transaction_code: r.transaction_code || null,
        asset: r.asset ? r.asset.replace(/:.+$/, "") : null,
        assetname: r.assetname || null,
        date: r.date
          ? DateTime.fromFormat(r.date, "yyyy-MM-dd HH:mm:ss").toISODate()
          : null,
        price: r.price ? Number(r.price) : null,
        ticker: r.ticker ? r.ticker.replace(/:.+$/, "") : null,
        quantity: r.quantity ? Number(r.quantity) : null,
        fee: r.fee ? Number(r.fee) : null,
        nkd: r.nkd ? Number(r.nkd) : null,
        nominal: r.nominal ? Number(r.nominal) : null,
        note: r.note || "-",
        currency: r.currency || null,
        type: r.type || null,
        operation: r.operation || null
      }));

      const csv = stringifyCsv(operations, {
        header: true
      });
      fs.writeFileSync(tempFile, csv);

      await conn.query(`truncate portfolio_operations`);
      await new Promise((resolve, reject) => {
        const stream = conn.query(
          copyFrom(`copy portfolio_operations from stdin with csv header;`)
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

    fs.unlinkSync(tempFile);

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
