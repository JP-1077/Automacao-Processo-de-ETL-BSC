# Automação Processo de ETL BSC

## Objetivo 🎯

Automatizar a ingestão de dados de monitoria da operação BlueSix, realizando limpeza, transformação, controle de duplicidade e carga na base final, com rastreabilidade via log.

## Tecnologias e Ferramentas 🛠

* **Banco de Dados:** SQL Server
* **Agendamento:** SQL Server Agent Jobs
* **Importação:** Via comando BULK INSERT
* **Excecução:** Por meio de Procedure (PR_IMPORT_MONITORIA_BLUESIX_BSC)

## System Design ✍🏼

