INSERT INTO instrument_stock(id, type, quantity, brand ) VALUES (1,'saxofon', 10, 'Yamaha' );
INSERT INTO instrument_stock(id, type, quantity, brand ) VALUES (2,'saxofon', 10, 'Gibson');
INSERT INTO instrument_stock(id, type, quantity, brand ) VALUES (3, 'guitarr', 20, 'Steinway Musical Instruments' );
INSERT INTO instrument_stock(id, type, quantity, brand ) VALUES (4, 'piano', 30, 'Roland' );
INSERT INTO instrument_stock(id, type, quantity, brand ) VALUES (5, 'flute', 10, 'Shure');


INSERT INTO renting_instruments (id, start_date, end_date, student_id, staff_id, instrument_id ) VALUES (1,'2020-01-04','2020-01-24',1,1,1);
INSERT INTO renting_instruments (id, start_date, end_date, student_id, staff_id, instrument_id ) VALUES (2,'2020-01-10','2020-01-15',2,1,2);
INSERT INTO renting_instruments (id, start_date, end_date, student_id, staff_id, instrument_id ) VALUES (3,'2020-01-11','2020-01-27',3,1,3);
INSERT INTO renting_instruments (id, start_date, end_date, student_id, staff_id, instrument_id ) VALUES (4,'2020-01-12','2020-01-25',4,2,4);
INSERT INTO renting_instruments (id, start_date, end_date, student_id, staff_id, instrument_id ) VALUES (5,'2020-02-04','2020-02-20',5,1,5);
INSERT INTO renting_instruments (id, start_date, end_date, student_id, staff_id, instrument_id ) VALUES (6,'2020-02-04','2020-02-14',6,1,1);
INSERT INTO renting_instruments(id, start_date, end_date, student_id, staff_id, instrument_id ) VALUES (7,'2020-02-07','2020-02-14',7,2,1);
INSERT INTO renting_instruments(id, start_date, end_date, student_id, staff_id, instrument_id ) VALUES (8,'2020-03-01','2020-03-14',8,1,1);
INSERT INTO renting_instruments(id, start_date, end_date, student_id, staff_id, instrument_id ) VALUES (9,'2020-03-02','2020-03-24',9,1,2);
INSERT INTO renting_instruments(id, start_date, end_date, student_id, staff_id, instrument_id ) VALUES (10,'2020-03-14','2020-04-08',10,2,4);
INSERT INTO renting_instruments(id, start_date, end_date, student_id, staff_id, instrument_id ) VALUES (11,'2020-04-02','2020-04-18',11,1,5);
INSERT INTO renting_instruments(id, start_date, end_date, student_id, staff_id, instrument_id ) VALUES (12,'2020-04-05','2020-04-19',12,1,1);
INSERT INTO renting_instruments(id, start_date, end_date, student_id, staff_id, instrument_id ) VALUES (13,'2020-04-09','2020-04-22',1,2,2);
INSERT INTO renting_instruments ( start_date, end_date, student_id, staff_id, instrument_id ) VALUES ('2020-01-04','2020-01-24',2,1,4);


INSERT INTO student_enrollment(id, instrument, skill, enrollment_id, accepted, staff_id) VALUES (1, 'piano', 'beginner', 'enr1', 'accepted', 1);
INSERT INTO student_enrollment(id, instrument, skill, enrollment_id, accepted, staff_id) VALUES (2, 'piano', 'intermidiate', 'enr2', 'declined', 1);
INSERT INTO student_enrollment(id, instrument, skill, enrollment_id, accepted, staff_id) VALUES (3, 'guitarr', 'advanced', 'enr3', 'accepted', 1);
INSERT INTO student_enrollment(id, instrument, skill, enrollment_id, accepted, staff_id) VALUES (4, 'guitarr', 'beginner', 'enr4', 'accepted', 2);
INSERT INTO student_enrollment(id, instrument, skill, enrollment_id, accepted, staff_id) VALUES (5, 'saxofon', 'beginner', 'enr5', 'accepted', 1);
INSERT INTO student_enrollment(id, instrument, skill, enrollment_id, accepted, staff_id) VALUES (6, 'trumpet', 'beginner','enr6', 'declined', 2);
INSERT INTO student_enrollment(id, instrument, skill, enrollment_id, accepted, staff_id) VALUES (7, 'trumpet', 'advanced', 'enr7', 'declined', 1);
INSERT INTO student_enrollment(id, instrument, skill, enrollment_id, accepted, staff_id) VALUES (8, 'piano', 'advanced', 'enr8', 'accepted', 1);
INSERT INTO student_enrollment(id, instrument, skill, enrollment_id, accepted, staff_id) VALUES (9, 'guitarr', 'advanced', 'enr9', 'accepted', 2);
INSERT INTO student_enrollment(id, instrument, skill, enrollment_id, accepted, staff_id) VALUES (10, 'flute', 'beginner', 'enr10', 'accepted', 2);