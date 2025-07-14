

/*
DESENVOLVEDOR: João Pedro Mendes Fonsec
Objetivo: Processo ETL automatizado para importar, tratar e carregar dados de monitoria da operação BlueSix.
Funcionalidades:
	- Realiza limpeza de tabelas temporárias e dados antigos.
	- Importa dados brutos de um arquivo CSV com codificação UTF-8 localizado em rede.
	- Trata e transforma os dados importados, ajustando formatos e tipagens.
	- Aplica regra de negócio para evitar duplicidade de dados na tabela final (eliminação por faixa de data).
	- Insere os dados tratados na tabela final TB_BSC_MONITORIA_B6.
	- Registra o log de execução em TB_PROCS_LOG para controle de execução.
	- Remove tabelas temporárias ao final do processo.

Execução: A procedure que armazena todo esse processo é denominada como PR_IMPORT_MONITORIA_BLUESIX_BSC e é executada diariamente por meio de um SQL Server Agent Job agendado.

Origem dos Dados: Arquivo CSV

Tabelas Envolvidas:
	- TEMPORÁRIAS: #TEMP e #STAGE
	- FINAL: TB_BSC_MONITORIA_B6
	- LOG: TB_PROCS_LOG
*/


/*=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-*/
																/* ETAPA 1: LIMPEZA DE STAGE E #TEMP */
/*=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-*/
--TRUNCATE TABLE STG_BSC_MONITORIA_B6

-- Remove a tabela temporária #TEMP se ela existir para garantir que a execução seja limpa
IF OBJECT_ID('tempdb..#TEMP', 'U') IS NOT NULL    DROP TABLE #TEMP;


/*=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-*/
															/* ETAPA 2: CONFIGURAÇÕES DE LOG */
/*=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-*/

-- Armazena o horário de início do processo.
DECLARE @START DATETIME = CAST(GETDATE() AS DATETIME)

-- Define o nome do processo para rastreamento de log.
DECLARE @PROCESS_NAME VARCHAR(MAX) = 'RCCM_IMPORT_MONITORIA_BLUESIX_BSC'


/*=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-*/
															/* ETAPA 3: BLOCO DE CARGA */
/*=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-*/

-- Criação da tabela temporária #TEMP que receberá os dados brutos do CSV.
CREATE TABLE #TEMP
	(
	CODIGO_AVALIACAO NVARCHAR(500),
    NEGOCIO NVARCHAR(500),
    CAMPANHA NVARCHAR(500),
    AVALIADO NVARCHAR(500),
    SUPERIOR NVARCHAR(500),
    DATA_AVALIACAO NVARCHAR(500),
    TIPO_AVALIACAO NVARCHAR(500),
    DEPARTAMENTO NVARCHAR(500),
    AVALIADOR NVARCHAR(500),
    PROCESSO NVARCHAR(500),
    CODIGO_GRAVACAO NVARCHAR(500),
    TEMPO_AVALIACAO NVARCHAR(500),
    NOTA NVARCHAR(500),
    CONCEITO NVARCHAR(500),
    DATA_FEEDBACK NVARCHAR(500),
    RESULTADO_FEEDBACK NVARCHAR(500),
    RESPONSAVEL_FEEDBACK NVARCHAR(500),
    OBS_RESPONSAVEL_FEEDBACK NVARCHAR(500),
    OBS_AVALIADO NVARCHAR(500),
    TABULACAO NVARCHAR(500),
    NOTA_SEM_NCG NVARCHAR(500),
    VISUALIZACAO_AVALIADO NVARCHAR(500),
    PESQUISA_AVALIADO NVARCHAR(500),
    LOGIN_AVALIADO NVARCHAR(500),
    DATA_ADMISSAO_AVALIADO NVARCHAR(500),
    DATA_INICIO_SETOR NVARCHAR(500),
    CODIGO_EXTERNO NVARCHAR(500),
    STATUS_FEEDBACK NVARCHAR(500),
    TEMPO_FEEDBACK NVARCHAR(500),
    FORMULARIO NVARCHAR(500),
    CODIGO_GRAVADOR NVARCHAR(500),
    CODIGO_INTEGRACAO NVARCHAR(500),
    DATA_CONTATO NVARCHAR(500),
    Aging NVARCHAR(500),
    Documento NVARCHAR(500),
    Frente NVARCHAR(500),
    Gerente NVARCHAR(500),
    Id_Cisco NVARCHAR(500),
    LOGIN_TOUF NVARCHAR(500),
    Motivo_1 NVARCHAR(500),
    Motivo_1_Ultrafibra NVARCHAR(500),
    Motivo_2 NVARCHAR(500),
    Motivo_2_Ultrafibra NVARCHAR(500),
    Motivo_3 NVARCHAR(500),
    Motivo_3_Ultrafibra NVARCHAR(500),
    Motivo_Agrupado NVARCHAR(500),
    Motivo_Agrupado_Ultrafibra NVARCHAR(500),
    Motivo_Contato_Ultrafibra NVARCHAR(500),
    Nome_Consultor NVARCHAR(500),
    Nome_Supervisor NVARCHAR(500),
    Processo_Status NVARCHAR(500),
    Registro_Siebel NVARCHAR(500),
    Resolucao_Protocolo NVARCHAR(500),
    Sentimento NVARCHAR(500),
    Sentimento_Transicao NVARCHAR(500),
    Silencio NVARCHAR(500),
    Tipo_Segmento NVARCHAR(500),
    TMA_Ligacao NVARCHAR(500),
    UF NVARCHAR(500),
    PERIODO NVARCHAR(500),
    PONTOS_POSITIVOS NVARCHAR(500),
    PONTOS_MELHORAR NVARCHAR(500),
    QTD_ARQUIVOS NVARCHAR(500),
    MATRICULA NVARCHAR(500),
    OBSERVACAO NVARCHAR(4000),
    EMAIL_AVALIADO NVARCHAR(500),
    TICKET NVARCHAR(500),
    ACIMA_META NVARCHAR(500),
    UNIDADE NVARCHAR(500),
    STATUS_USUARIO NVARCHAR(500),
    SEGUNDO_SUPERIOR NVARCHAR(500),
    TERCEIRO_SUPERIOR NVARCHAR(500),
    DATA_PRAZO_FEEDBACK NVARCHAR(500),
    FEEDBACK_NO_PRAZO NVARCHAR(500),
    PENDENTE_ASSINATURA NVARCHAR(500),
    DIAS_ATE_ASSINATURA NVARCHAR(500),
    DIAS_PENDENTES_ATE_ASSINATURA NVARCHAR(500),
    ULTIMA_ATUALIZACAO NVARCHAR(500)
	);
 


/*=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-*/
														   /* ETAPA 4: VARIAVEIS DE CONTROLE DE ARQUIVOS */
/*=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-*/

-- Armazena o caminho onde o arquivo CSV está localizado.
DECLARE @PATH NVARCHAR(MAX) = '\\SNEPDB56C01\Repositorio\BDS\0045 - IMPORTACAO_MONITORIA_QUALIDADE_BSC\0001 - ENTRADAS\'

-- Nome do arquivo a ser importado.
DECLARE @FILE NVARCHAR(MAX) = 'MonitoriaBlue6_BSC.CSV'

-- Concatena as duas variaveis acima para termos o caminho completo do arquivo.
DECLARE @FULLPATH NVARCHAR(MAX) = @PATH + @FILE

-- Comando de importação usando BULK INSERT com modificação UTF - 8 e delimitador ";".
DECLARE @SQL NVARCHAR(MAX) = ''
SET @SQL = N'

BULK INSERT #TEMP
FROM ''' + @FullPath + '''
WITH (
FIELDTERMINATOR = '';'',
ROWTERMINATOR = ''0x0a'',
FIRSTROW = 2,
CODEPAGE = ''65001''
    		)';

-- Executa o comando de importação 
EXEC SP_EXECUTESQL @SQL;


/*=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-*/
																/* ETAPA 5: TRATAMENTO */
/*=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-*/

-- Remove a tabela #STAGE, caso já exista.
IF OBJECT_ID('tempdb..#STAGE', 'U') IS NOT NULL    DROP TABLE #STAGE;

-- Transforma os dados da #TEMP:
	-- Removendo aspas
	-- Espaços em branco
	-- Convertendo para tipos de dados mais apropriados
SELECT
TRIM(REPLACE(AVALIADO, '"', '')) AS AVALIADO,
TRIM(REPLACE(SUPERIOR, '"', '')) AS SUPERIOR,
CONVERT(datetime, TRIM(REPLACE(DATA_AVALIACAO, '"', '')), 103) AS DATA_AVALIACAO,
TRY_CAST(REPLACE(TRIM(REPLACE(NOTA, '"', '')), ',', '.') AS real) AS NOTA,
TRIM(REPLACE(LOGIN_TOUF, '"', '')) AS [6858 Login (T ou F)],
CAST(
DATEFROMPARTS(
	YEAR(CONVERT(datetime, TRIM(REPLACE(DATA_AVALIACAO, '"', '')), 103)),
	MONTH(CONVERT(datetime, TRIM(REPLACE(DATA_AVALIACAO, '"', '')), 103)),
	1
) AS datetime
) AS PERIODO,
TRIM(REPLACE(UNIDADE, '"', '')) AS UNIDADE,
CAST(GETDATE() AS datetime) AS DTH_INPUT
INTO #STAGE
FROM #TEMP



/*=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-*/
																/* ETAPA 6: REGRA DE NEGOCIO: AJUSTE TABELA FINAL */
/*=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-*/


-- REGRA DE NEGOCIO: DELETAR DA TABELA FINAL TUDO AQUILO QUE ESTÁ SENDO CARREGADO DA STAGE QUE ESTÁ ENTRE A DATA MINIMA E MAXIMA DO CAMPO DATA_AVALIACAO.

-- Armazena a data mínima dos dados que serão inseridos.
DECLARE @MIN DATETIME = (SELECT MIN(DATA_AVALIACAO) FROM #STAGE)

-- Define a data máxima dos dados que serão inseridos.
DECLARE @MAX DATETIME = (SELECT MAX(DATA_AVALIACAO) FROM #STAGE)

-- Remove da tabela final os registros que estão dentro do mesmo intervalo de dados da carga atual.
DELETE FROM TB_BSC_MONITORIA_B6 WHERE DATA_AVALIACAO BETWEEN @MIN AND @MAX 


/*=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-*/
																/* ETAPA 7: LOAD */
/*=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-*/

-- Insere os dados tratados da #STAGE na tabela final.
INSERT INTO TB_BSC_MONITORIA_B6
SELECT * FROM #STAGE


/*=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-*/
																/* ETAPA 8: INSERÇÃO DE LOG */
/*=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-*/

-- Inserir um log na tabela de monitoramento de processos (TB_PROCS_LOG). Para termos um registro quando o processo for executado com sucesso.
insert into TB_PROCS_LOG
values(
	@PROCESS_NAME, --processo
	@START, --horario start
	cast(getdate() as datetime), -- horario end
	'OK', --status
	NULL -- frase descrição
)

/*=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-*/
																/* ETAPA 9: LIMPEZA DE STAGE, #TEMP E DELETE DO ARQUIVO */
/*=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-*/

-- Remove as tabelas temporárias utilizadas e a tabela de STAGE.
IF OBJECT_ID('tempdb..#TEMP', 'U') IS NOT NULL    DROP TABLE #TEMP;
IF OBJECT_ID('tempdb..#STAGE', 'U') IS NOT NULL    DROP TABLE #STAGE;
	





