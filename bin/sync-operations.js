const pg = require("pg");
const fs = require("fs");
const { DateTime } = require("luxon");
const parseCsv = require("csv-parse/lib/sync");
const stringifyCsv = require("csv-stringify/lib/sync");

const tempFile = process.cwd() + "/temp.csv";

async function main() {
  try {
    const conn = new pg.Client();
    await conn.connect();

    await conn.query(`truncate operations`);

    const files = fs.readdirSync("data");
    for (const file of files) {
      if (!/\.csv$/.test(file)) {
        continue;
      }

      const data = fs.readFileSync("data/" + file);
      const rows = parseCsv(data, {
        columns: true,
        delimiter: ";"
      }).map(row => ({
        operation_time: row["Дата операции"]
          ? DateTime.fromFormat(
              row["Дата операции"],
              "dd.MM.yyyy HH:mm:ss"
            ).toISO()
          : null,
        payment_date: row["Дата платежа"]
          ? DateTime.fromFormat(row["Дата платежа"], "dd.MM.yyyy").toISODate()
          : null,
        card: row["Номер карты"] || null,
        status: row["Статус"] || null,
        operation_amount: row["Сумма операции"]
          ? Number(row["Сумма операции"].replace(",", "."))
          : null,
        operation_currency: row["Валюта операции"] || null,
        payment_amount: row["Сумма платежа"]
          ? Number(row["Сумма платежа"].replace(",", "."))
          : null,
        payment_currency: row["Валюта платежа"] || null,
        cashback: row["Кэшбэк"]
          ? Number(row["Кэшбэк"].replace(",", "."))
          : null,
        category: row["Категория"] || "-",
        mcc: row["MCC"] ? Number(row["MCC"].replace(",", ".")) : null,
        description: row["Описание"],
        bonuses: row["Бонусы (включая кэшбэк)"]
          ? Number(row["Бонусы (включая кэшбэк)"].replace(",", "."))
          : null
      }));

      const csv = stringifyCsv(rows, {
        header: true
      });
      fs.writeFileSync(tempFile, csv);

      await conn.query(`copy operations from '${tempFile}' with csv header;`);
    }

    if (fs.existsSync(tempFile)) {
      fs.unlinkSync(tempFile);
    }

    await conn.query(`
      insert into operations_mapping
      select
          o.category,
          o.description,
          null as custom_category,
          null as custom_category_group
      from
          operations o
      where
          (select count(*) from operations_mapping om where om.category = o.category and om.description = o.description) = 0
      group by
          o.category,
          o.description
    `);

    const res = await conn.query(
      `select category, description from operations_mapping where custom_category is null`
    );

    if (res.rowCount > 0) {
      console.log("Новые категории");
      for (const row of res.rows) {
        console.log(`  ${row.category} — ${row.description}`);
      }
    }

    conn.end();
  } catch (err) {
    console.error(err);
    process.exit(1);
  }
}

main();
