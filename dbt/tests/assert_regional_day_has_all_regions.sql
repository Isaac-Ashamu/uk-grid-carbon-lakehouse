-- Each settlement period should carry all 18 regions.
select period_start, count(*) as regions
from {{ ref('fct_intensity_regional_halfhourly') }}
group by 1
having count(*) <> 18
