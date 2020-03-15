#!/usr/bin/env node
require("dotenv").config();
const pg = require("pg");
const chalk = require("chalk");
const argv = require("yargs").option("views", {
  array: true,
  string: true,
  default: ["portfolio_performance"]
}).argv;

async function main() {
  try {
    process.stdout.write(chalk.yellow("Refreshing materialized views\n"));

    const conn = new pg.Client({
      host: process.env.PSQL_HOST,
      port: process.env.PSQL_PORT,
      user: process.env.PSQL_USER,
      password: process.env.PSQL_PASS,
      database: process.env.PSQL_DB
    });
    await conn.connect();

    if (argv.views.includes("portfolio_performance")) {
      try {
        process.stdout.write("\tportfolio_performance ");
        await conn.query(`refresh materialized view portfolio_performance`);
        process.stdout.write(chalk.green("OK\n"));
      } catch (err) {
        process.stdout.write(chalk.red("ERROR\n"));
        throw err;
      }
    }

    conn.end();
  } catch (err) {
    console.error(err);
    process.exit(1);
  }
}

main();
