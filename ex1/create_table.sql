use new_schema;
DROP TABLE order_invoice_items;
CREATE TABLE order_invoice_items (
	order_id varchar(255),
    item_index int,
    shipment_id varchar(255),
    total_amount float
);

DELIMITER $$
DROP PROCEDURE IF EXISTS `insert_values_to_order_invoice_items` $$

-- procedure is used for insert data to order_invoice_items
CREATE PROCEDURE insert_values_to_order_invoice_items()
BEGIN
	DECLARE done INT DEFAULT FALSE;
	DECLARE order_id varchar(255);
    DECLARE json_data varchar(255);
    DECLARE cursors CURSOR FOR SELECT id, data from orders_raw_data;
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = True;
    OPEN cursors;
    readLoop: LOOP
		FETCH NEXT FROM cursors INTO order_id, json_data;
		IF done THEN
			Leave readLoop;
		END IF;
        INSERT INTO order_invoice_items(order_id, item_index,  shipment_id, total_amount)
-- use json_table function extract json data to table
		SELECT order_id, item_index-1 as item_index, shipment_id, total_amount
		FROM JSON_TABLE(json_data,
		"$.invoice_items[*]" COLUMNS(
			item_index FOR ORDINALITY, 
			shipment_id varchar(255) PATH "$.shipment_id" DEFAULT '1' ON EMPTY,
			total_amount float PATH "$.total_amount"
		)) extract_table;
	END LOOP;
    CLOSE cursors;
END $$
DELIMITER ;

CALL insert_values_to_order_invoice_items();