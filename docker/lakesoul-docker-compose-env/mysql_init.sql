create database if not exists test_cdc;

use test_cdc;

create table `default_init` (`id` int NOT NULL, PRIMARY KEY (`id`));
insert into default_init values(1);
