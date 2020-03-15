#!/usr/bin/env node
require("dotenv").config();
const pg = require("pg");
const chalk = require("chalk");
const cheerio = require("cheerio");
const fs = require("fs");
const fetch = require("node-fetch");
const { DateTime } = require("luxon");
const stringifyCsv = require("csv-stringify/lib/sync");
const copyFrom = require("pg-copy-streams").from;

const tempFile = process.cwd() + "/temp.csv";
const year = DateTime.local().year;

const months = {
  января: "01",
  февраля: "02",
  марта: "03",
  апреля: "04",
  мая: "05",
  июня: "06",
  июля: "07",
  августа: "08",
  сентября: "09",
  октября: "10",
  ноября: "11",
  декабря: "12"
};

async function login() {
  const body = `login=${encodeURIComponent(
    process.env.UTKONOS_EMAIL
  )}&password=${encodeURIComponent(process.env.UTKONOS_PASSWORD)}&pfb=dnd`;
  const res = await fetch("https://www.utkonos.ru/auth/request", {
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

async function getList(cookies) {
  const res = await fetch(
    `https://www.utkonos.ru/my-account/orders/year/${year}`,
    {
      headers: {
        Cookie: Object.entries(cookies)
          .map(([k, v]) => `${k}=${v}`)
          .join(";")
      }
    }
  );

  const html = await res.text();
  const $ = cheerio.load(html);

  return $(".order_view-id")
    .map((i, el) => $(el).text())
    .toArray();
}

async function get(cookies, id) {
  async function getBlankId() {
    const res = await fetch(`https://www.utkonos.ru/my-account/order/${id}`, {
      headers: {
        Cookie: Object.entries(cookies)
          .map(([k, v]) => `${k}=${v}`)
          .join(";")
      }
    });

    const html = await res.text();
    const $ = cheerio.load(html);

    return $(".page-control_blank a")
      .attr("href")
      .replace(/[^0-9]/g, "");
  }

  const blankId = await getBlankId();

  const res = await fetch(
    `https://www.utkonos.ru/my-account/orders/order/blank/${blankId}`,
    {
      headers: {
        Cookie: Object.entries(cookies)
          .map(([k, v]) => `${k}=${v}`)
          .join(";")
      }
    }
  );

  const html = await res.text();
  const $ = cheerio.load(html);

  const [day, month] = $("td:contains(Дата выдачи)")
    .parent()
    .find("td:last-child")
    .text()
    .split(/\s+/);
  const date = `${year}-${months[month]}-${day.padStart(2, "0")}`;

  const positions = $("table.positions")
    .find("tr")
    .toArray()
    .slice(1, -1)
    .map(el => ({
      order_id: id,
      date,
      name: $(el)
        .find("td:nth-child(1) a")
        .text()
        .trim(),
      id: Number(
        $(el)
          .find("td:nth-child(1) a")
          .attr("href")
          .replace(/[^0-9]/g, "")
      ),
      article: Number(
        $(el)
          .find("td:nth-child(2) a")
          .text()
      ),
      unit: $(el)
        .find("td:nth-child(3)")
        .text()
        .trim(),
      amount: Number.parseFloat(
        $(el)
          .find("td:nth-child(4)")
          .text()
          .replace(",", ".")
      ),
      price: Number.parseFloat(
        $(el)
          .find("td:nth-child(5)")
          .text()
          .replace(",", ".")
      ),
      total: Number.parseFloat(
        $(el)
          .find("td:nth-child(6)")
          .text()
          .replace(",", ".")
      )
    }));

  return positions;
}

async function main() {
  try {
    process.stdout.write(chalk.yellow("Syncing utkonos\n"));

    const conn = new pg.Client({
      host: process.env.PSQL_HOST,
      port: process.env.PSQL_PORT,
      user: process.env.PSQL_USER,
      password: process.env.PSQL_PASS,
      database: process.env.PSQL_DB
    });
    await conn.connect();

    const knownIds = (
      await conn.query(`select distinct order_id from utkonos_orders`)
    ).rows.map(row => row.order_id);

    const cookies = await login();
    const ids = (await getList(cookies)).filter(id => !knownIds.includes(id));

    for (const id of ids) {
      try {
        process.stdout.write(`\t${id} `);
        const positions = await get(cookies, id);
        await conn.query(`delete from utkonos_orders where order_id = $1`, [
          id
        ]);

        const csv = stringifyCsv(positions, {
          header: true
        });
        fs.writeFileSync(tempFile, csv);

        await new Promise((resolve, reject) => {
          const stream = conn.query(
            copyFrom(`copy utkonos_orders from stdin with csv header;`)
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

    await conn.query(`
      insert into utkonos_mapping
      select utkonos_orders.name,
            null as custom_category
      from utkonos_orders
      where (select count(*)
            from utkonos_mapping
            where utkonos_orders.name = utkonos_mapping.name) = 0
      group by name
      order by name collate "POSIX"
    `);

    conn.end();
  } catch (err) {
    console.error(err);
    process.exit(1);
  }
}

main();
