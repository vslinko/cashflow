node ./bin/sync-operations.js
node ./bin/sync-portfolio.js --no-refresh
node ./bin/sync-prices.js --no-refresh
node ./bin/sync-bonds.js --no-refresh
node ./bin/sync-utkonos.js
node ./bin/refresh-materialized-views.js
