--Pagos historicos vs ultimos pagos--
select f1.cuenta, f1.pago_historico, 
       f2.monto as ultimo_monto, f2.info_day
from (select a.cuenta,a.pago_historico , b.ultima_fecha
      from(select  cuenta, sum(monto) as pago_historico
           from data_lake.tp_reportes_pagos_brm
           --where cuenta='1.4272457' 
           group by cuenta) as a
      join (select cuenta,max(info_day) as ultima_fecha
            from data_lake.tp_reportes_pagos_brm
            --where cuenta='1.4272457'
            group by cuenta) as b
      on a.cuenta=b.cuenta) as f1
left join (select  cuenta, monto, info_day
           from data_lake.tp_reportes_pagos_brm) as f2
on f1.cuenta=f2.cuenta
where f1.ultima_fecha=f2.info_day
limit 100;
