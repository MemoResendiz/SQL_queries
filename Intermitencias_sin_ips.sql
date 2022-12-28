--cantidad de cuentas con cada paquete con interrupciones e intermitencias,
--que hacemos con el 98?
--historico de esto
select cuenta,productid,
             to_timestamp (begintime, 'YYYYMMDDHH24MISS',true) as begin_completa,
             to_char(begin_completa,'HH24:MI:SS') as begin_hora,
             to_timestamp (endtime, 'YYYYMMDDHH24MISS',true) as end_completa,
             to_char(end_completa,'HH24:MI:SS') as end_hora,
             DATEDIFF(sec,begin_completa::timestamp,end_completa::timestamp) as segundo_transcurrido,
             substring(begintime,9,2) as hora,
             (substring(productid,2,4)*0.1)::float  as Subida_paquete,
            substring(productid,2,4)::float as Bajada_paquete,
            (Subida_paquete * segundo_transcurrido)  as total_megas_subida,
            (Bajada_paquete * segundo_transcurrido)  as total_megas_bajada,
            case 
              when segundo_transcurrido >= 899 THEN 'OK'
              when  segundo_transcurrido = 898 THEN '98'
              when segundo_transcurrido >=1 and segundo_transcurrido < 898 then 'interrupcion'
              when segundo_transcurrido = 0 then 'intermitencia' 
             END as FALLAS
            from data_lake.edr_aaa
         where info_day =20221129
         --and segundo_transcurrido < 899
         --and segundo_transcurrido = 900
         --and cuenta='1.4272457'
         and cuenta='0100014525'
         limit 1000;
