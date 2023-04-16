with
    fonte_funcionarios as (
        select *
        from {{ source('erp', 'employees') }}
    )

    , renomear as (
        select
            cast(employee_id as int) as id_funcionario
            , cast(reports_to as int) as id_gerente				
            , cast((first_name || ' ' || last_name) as string) as funcionario						
            , cast(birth_date as date) as funcionario_data_nasc			
            , cast(hire_date as date) as funcionario_data_contr	
            , cast(address as string) as funcionario_endereco
            , cast(city as string) as funcionario_cidade
            , cast(region as string) as funcionario_regiao
            , cast(postal_code as string) as funcionario_cep				
            , cast(country as string) as funcionario_pais				
            , cast(notes as string) as funcionario_notas
				
        from fonte_funcionarios
    )
select *
from renomear