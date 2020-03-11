#!/usr/bin/env node
require("dotenv").config();
const pg = require("pg");
const cheerio = require("cheerio");
const fs = require("fs");
const fetch = require("node-fetch");
const { DateTime } = require("luxon");
const stringifyCsv = require("csv-stringify/lib/sync");
const copyFrom = require("pg-copy-streams").from;

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

async function get(cookies, bond) {
  const res = await fetch(`https://blackterminal.ru/bonds/${bond}`, {
    headers: {
      Cookie: Object.entries(cookies)
        .map(([k, v]) => `${k}=${v}`)
        .join(";")
    }
  });
  return await res.text();
}

async function main() {
  try {
    const conn = new pg.Client({
      host: process.env.PSQL_HOST,
      port: process.env.PSQL_PORT,
      user: process.env.PSQL_USER,
      password: process.env.PSQL_PASS,
      database: process.env.PSQL_DB
    });
    await conn.connect();

    await conn.query(`truncate portfolio_bonds_payments`);

    const bonds = (
      await conn.query(
        `select distinct ticker from portfolio_operations where ticker like 'RU000A%'`
      )
    ).rows.map(row => row.ticker);

    for (const bond of bonds) {
      const cookies = await login();
      const result = await get(cookies, bond);

      const $ = cheerio.load(result);
      const rows = $(".widget-header:contains(График выплаты купонов)")
        .next(".widget-text")
        .find("table tr")
        .toArray()
        .map(el => {
          return $(el)
            .find("td,th")
            .map((i, el) => {
              return $(el)
                .text()
                .replace(/\s/g, "");
            })
            .toArray();
        })
        .reduce(
          (acc, row) => {
            if (!acc.headers) {
              acc.headers = row.reduce((acc, row, i) => {
                acc[row] = i;
                return acc;
              }, {});
            } else {
              acc.rows.push({
                payment_date: DateTime.fromFormat(
                  row[acc.headers["Датавыплаты"]],
                  "dd.MM.yyyy"
                ).toISODate(),
                coupon: row[acc.headers["Ставкакупона"]],
                payment: row[acc.headers["Размервыплаты"]],
                percent: row[acc.headers["%отноминала"]],
                nominal: row[acc.headers["номинал"]]
              });
            }

            return acc;
          },
          { headers: null, rows: [] }
        )
        .rows.map(row => ({
          bond,
          payment_date: row.payment_date,
          coupon: row.coupon ? Number.parseFloat(row.coupon) : null,
          payment: row.payment ? Number.parseFloat(row.payment) : null,
          percent: row.percent ? Number.parseFloat(row.percent) : null,
          nominal: row.nominal ? Number.parseFloat(row.nominal) : null
        }));

      const startDateRow = $('td:contains(Дата начала торгов)').text()
      const [date] = /\d{2}\.\d{2}\.\d{4}/.exec(startDateRow);
      rows.unshift({
        bond,
        payment_date: DateTime.fromFormat(date, 'dd.MM.yyyy').toISODate(),
        coupon: null,
        payment: null,
        percent: null,
        nominal: null
      });

      const csv = stringifyCsv(rows, {
        header: true
      });
      fs.writeFileSync(tempFile, csv);

      await new Promise((resolve, reject) => {
        const stream = conn.query(
          copyFrom(`copy portfolio_bonds_payments from stdin with csv header;`)
        );
        const fileStream = fs.createReadStream(tempFile);
        fileStream.on("error", reject);
        stream.on("error", reject);
        stream.on("end", resolve);
        fileStream.pipe(stream);
      });
    }

    if (fs.existsSync(tempFile)) {
      fs.unlinkSync(tempFile);
    }

    await conn.query(`refresh materialized view portfolio_performance`);

    conn.end();
  } catch (err) {
    console.error(err);
    process.exit(1);
  }
}

main();
