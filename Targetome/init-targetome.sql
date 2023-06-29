-- This SQL script initializes the targetome database and user

drop database if exists targetome;
create database targetome;
grant usage on *.* to 'user'@'localhost';
drop user 'user'@'localhost';
flush privileges;
create user 'user'@'localhost' identified by '';
grant all privileges on targetome.* to 'user'@'localhost';
grant file on *.* to 'user'@'localhost';
flush privileges;
