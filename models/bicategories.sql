--Neste exeplo, é filtrada a consulta pela variável "category" que foi criada no arquivo "dbt_project-yml" com o valor padrão "Seafood"
--Para sobrescrever, bast informar na linha de execução, 
-- ex: dbt run --select bicategories --vars "category: Condiments"
select {{retorna_campos()}}
  from {{ref('joins')}}
    where category_name = '{{var('category')}}'