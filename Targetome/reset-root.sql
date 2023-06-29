-- This SQL script resets the root user

drop user 'root'@'localhost';
create user 'root'@'localhost' identified by '';
grant all privileges on *.* to 'root'@'localhost' with grant option;
flush privileges;
