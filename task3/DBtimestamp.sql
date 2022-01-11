CREATE TABLE contacts_details (
 id INT NOT NULL,
 name VARCHAR(100),
 personal_number VARCHAR(50) NOT NULL,
 age VARCHAR(100),
 city VARCHAR(100),
 zip VARCHAR(100),
 street VARCHAR(100),
 role VARCHAR(100)
);

ALTER TABLE contacts_details ADD CONSTRAINT PK_contacts_details PRIMARY KEY (id);


CREATE TABLE email (
 id INT NOT NULL,
 eamil_adress VARCHAR(100)
);

ALTER TABLE email ADD CONSTRAINT PK_email PRIMARY KEY (id);


CREATE TABLE instructor (
 id INT NOT NULL,
 instructor_id VARCHAR(50) NOT NULL,
 contacts_details_id INT
);

ALTER TABLE instructor ADD CONSTRAINT PK_instructor PRIMARY KEY (id);


CREATE TABLE instrument_stock (
 id INT NOT NULL,
 type VARCHAR(100),
 quantity INT,
 brand VARCHAR(100)
);

ALTER TABLE instrument_stock ADD CONSTRAINT PK_instrument_stock PRIMARY KEY (id);


CREATE TABLE lesson (
 id INT NOT NULL,
 level VARCHAR(100),
 date TIMESTAMP(6),
 instrument_type VARCHAR(100),
 lesson_type VARCHAR(100),
 instructor_id INT
);

ALTER TABLE lesson ADD CONSTRAINT PK_lesson PRIMARY KEY (id);


CREATE TABLE price_scheme_instructor (
 id INT NOT NULL,
 advanced_price_instructor INT,
 intermediate_price_intructor INT,
 beginner_price_instructor INT
);

ALTER TABLE price_scheme_instructor ADD CONSTRAINT PK_price_scheme_instructor PRIMARY KEY (id);


CREATE TABLE price_scheme_student (
 id INT NOT NULL,
 advanced_price_student INT,
 intermediate_price_student INT,
 beginner_price_student INT,
 renting_price_per_day INT
);

ALTER TABLE price_scheme_student ADD CONSTRAINT PK_price_scheme_student PRIMARY KEY (id);


CREATE TABLE staff (
 id INT NOT NULL,
 staff_id VARCHAR(50) NOT NULL,
 contacts_details_id INT NOT NULL
);

ALTER TABLE staff ADD CONSTRAINT PK_staff PRIMARY KEY (id);


CREATE TABLE student (
 id INT NOT NULL,
 student_id VARCHAR(50) NOT NULL,
 parent_name VARCHAR(100) NOT NULL,
 family_id_for_discount VARCHAR(10),
 contacts_details_id INT
);

ALTER TABLE student ADD CONSTRAINT PK_student PRIMARY KEY (id);


CREATE TABLE student_enrollment (
 id INT NOT NULL,
 instrument VARCHAR(100),
 skill VARCHAR(100),
 enrollment_id VARCHAR(100),
 accepted VARCHAR(10),
 staff_id INT
);

ALTER TABLE student_enrollment ADD CONSTRAINT PK_student_enrollment PRIMARY KEY (id);


CREATE TABLE telephone_number (
 id INT NOT NULL,
 number VARCHAR(100),
 parent_number VARCHAR(100)
);

ALTER TABLE telephone_number ADD CONSTRAINT PK_telephone_number PRIMARY KEY (id);


CREATE TABLE contact_detail_email (
 email_id INT NOT NULL,
 contacts_details_id INT NOT NULL
);

ALTER TABLE contact_detail_email ADD CONSTRAINT PK_contact_detail_email PRIMARY KEY (email_id,contacts_details_id);


CREATE TABLE contact_detail_phone (
 telefone_number_id INT NOT NULL,
 contacts_details_id INT NOT NULL
);

ALTER TABLE contact_detail_phone ADD CONSTRAINT PK_contact_detail_phone PRIMARY KEY (telefone_number_id,contacts_details_id);


CREATE TABLE group_lesson (
 lesson_id INT NOT NULL,
 group_lesson_type VARCHAR(50),
 maximum_number_of_students INT,
 minimum_number_of_students INT
);

ALTER TABLE group_lesson ADD CONSTRAINT PK_group_lesson PRIMARY KEY (lesson_id);


CREATE TABLE indvidual_lesson (
 lesson_id INT NOT NULL,
 student_id INT NOT NULL
);

ALTER TABLE indvidual_lesson ADD CONSTRAINT PK_indvidual_lesson PRIMARY KEY (lesson_id,student_id);


CREATE TABLE price_for_instructor (
 price_scheme_instructor_id INT NOT NULL,
 instructor_id INT NOT NULL
);

ALTER TABLE price_for_instructor ADD CONSTRAINT PK_price_for_instructor PRIMARY KEY (price_scheme_instructor_id,instructor_id);


CREATE TABLE price_for_student (
 price_scheme_student_id INT NOT NULL,
 student_id INT NOT NULL
);

ALTER TABLE price_for_student ADD CONSTRAINT PK_price_for_student PRIMARY KEY (price_scheme_student_id,student_id);


CREATE TABLE renting_instruments (
 id SERIAL PRIMARY KEY,
 start_date TIMESTAMP(6) NOT NULL,
 end_date TIMESTAMP(6) NOT NULL,
 student_id INT NOT NULL,
 staff_id INT NOT NULL,
 instrument_id INT NOT NULL
);

-- ALTER TABLE renting_instruments ADD CONSTRAINT PK_renting_instruments PRIMARY KEY (id);


CREATE TABLE student_group_lesson (
 lesson_id INT NOT NULL,
 student_id INT NOT NULL
);

ALTER TABLE student_group_lesson ADD CONSTRAINT PK_student_group_lesson PRIMARY KEY (lesson_id,student_id);


CREATE TABLE ensemble (
 lesson_id INT NOT NULL,
 genre VARCHAR(100),
 maximum_number_of_students INT,
 minimum_number_of_students INT
);

ALTER TABLE ensemble ADD CONSTRAINT PK_ensemble PRIMARY KEY (lesson_id);


CREATE TABLE student_ensemble (
 lesson_id INT NOT NULL,
 student_id INT NOT NULL
);

ALTER TABLE student_ensemble ADD CONSTRAINT PK_student_ensemble PRIMARY KEY (lesson_id,student_id);


ALTER TABLE instructor ADD CONSTRAINT FK_instructor_0 FOREIGN KEY (contacts_details_id) REFERENCES contacts_details (id);


ALTER TABLE lesson ADD CONSTRAINT FK_lesson_0 FOREIGN KEY (instructor_id) REFERENCES instructor (id);


ALTER TABLE staff ADD CONSTRAINT FK_staff_0 FOREIGN KEY (contacts_details_id) REFERENCES contacts_details (id);


ALTER TABLE student ADD CONSTRAINT FK_student_0 FOREIGN KEY (contacts_details_id) REFERENCES contacts_details (id);


ALTER TABLE student_enrollment ADD CONSTRAINT FK_student_enrollment_0 FOREIGN KEY (staff_id) REFERENCES staff (id);


ALTER TABLE contact_detail_email ADD CONSTRAINT FK_contact_detail_email_0 FOREIGN KEY (email_id) REFERENCES email (id);
ALTER TABLE contact_detail_email ADD CONSTRAINT FK_contact_detail_email_1 FOREIGN KEY (contacts_details_id) REFERENCES contacts_details (id);


ALTER TABLE contact_detail_phone ADD CONSTRAINT FK_contact_detail_phone_0 FOREIGN KEY (telefone_number_id) REFERENCES telephone_number (id);
ALTER TABLE contact_detail_phone ADD CONSTRAINT FK_contact_detail_phone_1 FOREIGN KEY (contacts_details_id) REFERENCES contacts_details (id);


ALTER TABLE group_lesson ADD CONSTRAINT FK_group_lesson_0 FOREIGN KEY (lesson_id) REFERENCES lesson (id);


ALTER TABLE indvidual_lesson ADD CONSTRAINT FK_indvidual_lesson_0 FOREIGN KEY (lesson_id) REFERENCES lesson (id);
ALTER TABLE indvidual_lesson ADD CONSTRAINT FK_indvidual_lesson_1 FOREIGN KEY (student_id) REFERENCES student (id);


ALTER TABLE price_for_instructor ADD CONSTRAINT FK_price_for_instructor_0 FOREIGN KEY (price_scheme_instructor_id) REFERENCES price_scheme_instructor (id);
ALTER TABLE price_for_instructor ADD CONSTRAINT FK_price_for_instructor_1 FOREIGN KEY (instructor_id) REFERENCES instructor (id);


ALTER TABLE price_for_student ADD CONSTRAINT FK_price_for_student_0 FOREIGN KEY (price_scheme_student_id) REFERENCES price_scheme_student (id);
ALTER TABLE price_for_student ADD CONSTRAINT FK_price_for_student_1 FOREIGN KEY (student_id) REFERENCES student (id);


ALTER TABLE renting_instruments ADD CONSTRAINT FK_renting_instruments_0 FOREIGN KEY (student_id) REFERENCES student (id);
ALTER TABLE renting_instruments ADD CONSTRAINT FK_renting_instruments_1 FOREIGN KEY (staff_id) REFERENCES staff (id);
ALTER TABLE renting_instruments ADD CONSTRAINT FK_renting_instruments_2 FOREIGN KEY (instrument_id) REFERENCES instrument_stock (id);


ALTER TABLE student_group_lesson ADD CONSTRAINT FK_student_group_lesson_0 FOREIGN KEY (lesson_id) REFERENCES group_lesson (lesson_id);
ALTER TABLE student_group_lesson ADD CONSTRAINT FK_student_group_lesson_1 FOREIGN KEY (student_id) REFERENCES student (id);


ALTER TABLE ensemble ADD CONSTRAINT FK_ensemble_0 FOREIGN KEY (lesson_id) REFERENCES lesson (id);



ALTER TABLE student_ensemble ADD CONSTRAINT FK_student_ensemble_0 FOREIGN KEY (lesson_id) REFERENCES ensemble (lesson_id);
ALTER TABLE student_ensemble ADD CONSTRAINT FK_student_ensemble_1 FOREIGN KEY (student_id) REFERENCES student (id);


