SELECT
*
FROM TGFCAB 
WHERE
PENDENTE = 'S' 
	AND CODVEND = 14 
	AND CODTIPOPER = :CODTIPOPER
	AND NUNOTA = :NUNOTA
	AND NOT EXISTS (SELECT 1 FROM TGFVAR V WHERE TGFCAB.NUNOTA = V.NUNOTAORIG)
	AND DTNEG >= '02/02/2022' AND CODEMP = 1
	--AND VLRDESCTOT = 0 AND VLRDESCTOTITEM = 0