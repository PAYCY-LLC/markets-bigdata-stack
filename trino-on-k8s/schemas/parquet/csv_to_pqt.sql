insert into etl.blocks select * from default.blocks where start_block like '129%';
insert into etl.logs select * from default.logs where start_block like '129%';
insert into etl.transactions select * from default.transactions where start_block like '129%';
insert into etl.receipts select * from default.receipts where start_block like '129%';
insert into etl.token_transfers select * from default.token_transfers where start_block like '129%';
insert into etl.contracts select * from default.contracts where start_block like '129%';
insert into etl.tokens select * from default.tokens where start_block like '129%';
