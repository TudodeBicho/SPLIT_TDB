SELECT
*
FROM TGFCAB 
WHERE
PENDENTE = 'S' 
	AND CODVEND = 14 
	AND CODTIPOPER = :CODTIPOPER
	--AND NOT EXISTS (SELECT 1 FROM TGFVAR V WHERE TGFCAB.NUNOTA = V.NUNOTAORIG)
	AND (NOT EXISTS (SELECT 1 FROM TGFVAR V WHERE TGFCAB.NUNOTA = V.NUNOTAORIG) AND NOT EXISTS (SELECT 1 FROM TGFCAB C WHERE C.CODTIPOPER = 3132 AND C.BH_CODMKT = TGFCAB.BH_CODMKT))
	AND DTNEG >= '02/02/2022' AND CODEMP = 1
	--AND NUNOTA = 2596909
	--AND VLRDESCTOT = 0 AND VLRDESCTOTITEM = 0