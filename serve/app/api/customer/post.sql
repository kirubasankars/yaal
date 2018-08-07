--params(name, city)--

insert into customer(name, city, active) values({{name}},{{city}}, 1); select last_insert_rowid();
