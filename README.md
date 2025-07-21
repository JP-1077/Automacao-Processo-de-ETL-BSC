# Automação Processo de ETL BSC

## Objetivo 🎯

Automatizar a ingestão de dados de monitoria da operação BlueSix, realizando limpeza, transformação, controle de duplicidade e carga na base final, com rastreabilidade via log.

## Tecnologias e Ferramentas 🛠

* **Banco de Dados:** SQL Server
* **Agendamento:** SQL Server Agent Jobs
* **Importação:** Via comando BULK INSERT
* **Excecução:** Por meio de Procedure (PR_IMPORT_MONITORIA_BLUESIX_BSC)

## System Design ✍🏼

![Pipeline](Pipeline(3).png)

1. Importação: Leitura do CSV via BULK INSERT para #TEMP.
2. Transformação: Criação da #STAGE com limpeza de dados, conversões e formatação.
3. Regra de negócio: Exclusão de dados antigos da tabela final com base no intervalo de datas da carga atual.
4. Carga final: Inserção dos dados da #STAGE na TB_BSC_MONITORIA_B6.
5. Log de execução: Registro em TB_PROCS_LOG.
6. Limpeza final: Remoção das tabelas temporárias.


## Detalhes Técnicos ⚙

