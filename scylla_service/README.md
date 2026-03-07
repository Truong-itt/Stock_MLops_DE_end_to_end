sudo nano /etc/sysctl.d/99-database.conf


thêm cấu hình 
fs.aio-max-nr = 1048576


docker exec scylla-node1 cqlsh -e "SELECT COUNT(*) FROM stock_data.stock_prices;" 2>&1

docker exec scylla-node1 cqlsh -e "
SELECT COUNT(*) AS total_latest FROM stock_data.stock_latest_prices;
"
docker exec scylla-node1 cqlsh -e "SELECT * FROM stock_data.stock_prices LIMIT 5;"

docker exec scylla-node1 cqlsh -e "SELECT * FROM stock_data.stock_prices LIMIT 5;"

docker exec scylla-node1 cqlsh -e "SELECT * FROM stock_data.stock_latest_prices LIMIT 5;"