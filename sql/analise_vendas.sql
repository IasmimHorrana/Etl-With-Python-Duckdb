SELECT 
	data_venda,
	count(*) AS total_vendas, 
	SUM(valor) AS valor_total
FROM vendas v 
GROUP BY data_venda 
ORDER BY data_venda;
	
