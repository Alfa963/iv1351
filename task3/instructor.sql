INSERT INTO contacts_details (id,name, personal_number,age,city,zip,street,role) VALUES (21,'Alex Olsson', 8906071257, 32, 'Stockholm', 12355, 'Sveavägen 1','instructor');
INSERT INTO contacts_details (id,name, personal_number,age,city,zip,street,role) VALUES (22,'Amadu Bah', 8711217431, 34, 'Stockholm', 13187, 'Malmvägen 35','instructor');
INSERT INTO contacts_details (id,name, personal_number,age,city,zip,street,role) VALUES (23,'Abbe Svensson', 8610250215, 35, 'Stockholm', 16411, 'Brungatan 2','instructor');
INSERT INTO contacts_details (id,name, personal_number,age,city,zip,street,role) VALUES (24,'Kristian Nilson', 8704091563, 34, 'Stockholm', 14211, 'Vitbacken 94','instructor');
INSERT INTO contacts_details (id,name, personal_number,age,city,zip,street,role) VALUES (25,'Helena Holmsten', 8801035896, 33, 'Stockholm', 12112, 'Gulkullan 22','instructor');

INSERT INTO instructor  (id,instructor_id, contacts_details_id) VALUES (1,'INS01', 21);
INSERT INTO instructor  (id,instructor_id, contacts_details_id) VALUES (2,'INS02', 22);
INSERT INTO instructor  (id,instructor_id, contacts_details_id) VALUES (3,'INS03', 23);
INSERT INTO instructor  (id,instructor_id, contacts_details_id) VALUES (4,'INS04', 24);
INSERT INTO instructor  (id,instructor_id, contacts_details_id) VALUES (5,'INS05', 25);

INSERT INTO telephone_number (id, number, parent_number) VALUES (21,'07702494333','-');
INSERT INTO telephone_number (id, number, parent_number) VALUES (22,'07705684555','-');
INSERT INTO telephone_number (id, number, parent_number) VALUES (23,'07701479777','-');
INSERT INTO telephone_number (id, number, parent_number) VALUES (24,'07703658999','-');
INSERT INTO telephone_number (id, number, parent_number) VALUES (25,'07705897000','-');

INSERT INTO email (id,eamil_adress) VALUES (21,'Alex.Olsson@soundgood.se');
INSERT INTO email (id,eamil_adress) VALUES (22,'Amadu.Bah@soundgood.se');
INSERT INTO email (id,eamil_adress) VALUES (23,'Abbe.Svensson@soundgood.se');
INSERT INTO email (id,eamil_adress) VALUES (24,'Kristian.Nilson@soundgood.se');
INSERT INTO email (id,eamil_adress) VALUES (25,'Helena.Holmsten@soundgood.se');

INSERT INTO contact_detail_email (email_id,contacts_details_id) VALUES (21,21);
INSERT INTO contact_detail_email (email_id,contacts_details_id) VALUES (22,22);
INSERT INTO contact_detail_email (email_id,contacts_details_id) VALUES (23,23);
INSERT INTO contact_detail_email (email_id,contacts_details_id) VALUES (24,24);
INSERT INTO contact_detail_email (email_id,contacts_details_id) VALUES (25,25);


INSERT INTO contact_detail_phone (telefone_number_id,contacts_details_id) VALUES (21,21);
INSERT INTO contact_detail_phone (telefone_number_id,contacts_details_id) VALUES (22,22);
INSERT INTO contact_detail_phone (telefone_number_id,contacts_details_id) VALUES (23,23);
INSERT INTO contact_detail_phone (telefone_number_id,contacts_details_id) VALUES (24,24);
INSERT INTO contact_detail_phone (telefone_number_id,contacts_details_id) VALUES (25,25);