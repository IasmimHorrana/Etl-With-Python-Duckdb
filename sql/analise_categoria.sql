SELECT 
	categoria,
	count(*) AS total_vendas, 
	SUM(valor) AS valor_total
FROM vendas v 
GROUP BY categoria 
ORDER BY valor_total DESC;
	
