--# Fase de Importação dos dados de trabalho remoto.

with sources as (
    SELECT
        "jobTitle",
        "companyName",
        "jobType",
        "jobGeo",
        "jobLevel",
        "annualSalaryMin",
        "annualSalaryMax",
        "salaryCurrency"
    FROM {{source("PROJETO_DBT","listagem_trabalhos_remotos")}}
),

--# Fase de renamed, onde iremos inserir todas as transformações necessárias para a construção do modelo final, como por exemplo, renomear as colunas, retirar os espaços, etc.
renamed as (
    SELECT
        "jobTitle" as titulo_vaga,
        "companyName" as nome_empresa,
        "jobType" as tipo_trabalho,
        "jobGeo" as localizacao,
        "jobLevel" as senioridade,
        try_cast("annualSalaryMin" as float)  as minimo_salario_anual,
        try_cast("annualSalaryMax" as float) as maximo_salario_anual,
        "salaryCurrency" as moeda
    FROM sources
),

--# Final 

final as (
    SELECT
        titulo_vaga,
        nome_empresa,
        tipo_trabalho,
        localizacao,
        senioridade,
        Round(minimo_salario_anual / 12, 2) as minimo_salario_mensal,
        minimo_salario_anual,
        Round(maximo_salario_anual / 12, 2) as maximo_salario_mensal,
        maximo_salario_anual,
        moeda
    FROM renamed
    WHERE minimo_salario_anual is not null
    or maximo_salario_anual is not null
)

SELECT * FROM final