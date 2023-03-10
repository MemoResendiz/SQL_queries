SELECT b.idcuentabrm__c, b.codigopostalinstalacion__c, a.d_estado, a.d_ciudad, c.cambio_de_colonia
--cp, estado y ciudad de sepomex
FROM (SELECT  d_codigo, d_estado, d_ciudad
      FROM nuevos_negocios.sepomex 
      GROUP BY   d_codigo, d_estado, d_ciudad) AS a      
--cp de la ultima instalacion de las cuentas
JOIN (SELECT c1.idcuentabrm__c, c2.codigopostalinstalacion__c
      FROM (SELECT idcuentabrm__c, max(info_day) AS ultima_fecha
            FROM data_staging.slf_cuentafactura__c 
            WHERE info_day between '20220101' and '20221124' 
                  AND idcuentabrm__c in ('0104555090','0104561319') 
            GROUP BY idcuentabrm__c) AS c1
      JOIN (SELECT idcuentabrm__c, codigopostalinstalacion__c, info_day
            FROM data_staging.slf_cuentafactura__c 
            WHERE info_day between '20220101' and '20221124' )AS c2
      ON c1.idcuentabrm__c=c2.idcuentabrm__c
      WHERE c1.ultima_fecha=c2.info_day) AS b
ON a.d_codigo=b.codigopostalinstalacion__c
--cuantas veces a cambiado de colonia una cuenta
JOIN (SELECT idcuentabrm__c, COUNT(coloniainstalacion__c) AS cambio_de_colonia
      FROM (SELECT co2.idcuentabrm__c, co2.coloniainstalacion__c
            FROM (SELECT idcuentabrm__c, coloniainstalacion__c
                  FROM data_staging.slf_cuentafactura__c
                  WHERE info_day BETWEEN '20220101' AND '20221125'
                        AND idcuentabrm__c in ('0104555090','0104561319')) AS co1
            JOIN (SELECT idcuentabrm__c, coloniainstalacion__c
                  FROM data_staging.slf_cuentafactura__c
                  WHERE idcuentabrm__c in ('0104555090','0104561319')) AS co2
            ON co1.idcuentabrm__c=co2.idcuentabrm__c   
            GROUP BY co2.idcuentabrm__c,co2.coloniainstalacion__c) AS co3
      GROUP BY idcuentabrm__c) AS c
ON b.idcuentabrm__c=c.idcuentabrm__c;
