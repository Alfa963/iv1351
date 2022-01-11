INSERT INTO contacts_details (id,name, personal_number,age,city,zip,street,role) VALUES (26,'Ahmed Silva', 8506011247, 36, 'Stockholm', 12322, 'drotninggatan 1','staff');
INSERT INTO contacts_details (id,name, personal_number,age,city,zip,street,role) VALUES (27,'Carlos Hernandez', 8111217431, 40, 'Stockholm', 13147, 'svartgatan 39','staff');


INSERT INTO staff  (id,staff_id, contacts_details_id) VALUES (1,'STAf01', 26);
INSERT INTO staff  (id,staff_id, contacts_details_id) VALUES (2,'STAf02', 27);


INSERT INTO telephone_number (id, number, parent_number) VALUES (26,'07702498012','-');
INSERT INTO telephone_number (id, number, parent_number) VALUES (27,'07705444555','-');


INSERT INTO email (id,eamil_adress) VALUES (26,'ahmed.silva@soundgood.se');
INSERT INTO email (id,eamil_adress) VALUES (27,'carlos.hernandez@soundgood.se');


INSERT INTO contact_detail_email (email_id,contacts_details_id) VALUES (26,26);
INSERT INTO contact_detail_email (email_id,contacts_details_id) VALUES (27,27);



INSERT INTO contact_detail_phone (telefone_number_id,contacts_details_id) VALUES (26,26);
INSERT INTO contact_detail_phone (telefone_number_id,contacts_details_id) VALUES (27,27);
