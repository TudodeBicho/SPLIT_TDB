package br.com.sattva.tdb.acoes;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import com.sankhya.util.BigDecimalUtil;
import com.sankhya.util.TimeUtils;

import br.com.sankhya.extensions.actionbutton.AcaoRotinaJava;
import br.com.sankhya.extensions.actionbutton.ContextoAcao;
import br.com.sankhya.jape.EntityFacade;
import br.com.sankhya.jape.core.JapeSession;
import br.com.sankhya.jape.core.JapeSession.SessionHandle;
import br.com.sankhya.jape.sql.NativeSql;
import br.com.sankhya.jape.vo.DynamicVO;
import br.com.sankhya.jape.wrapper.JapeFactory;
import br.com.sankhya.jape.wrapper.JapeWrapper;
import br.com.sankhya.modelcore.util.EntityFacadeFactory;
import br.com.sattva.tdb.job.Split;
import br.com.sattva.tdb.job.Transferencia;
import br.com.sattva.tdb.utils.TdbHelper;

public class SplitPedidosAcaoII implements AcaoRotinaJava {
	public String log = "";
	public String regraPrioridade = "";
	public String statusProcessamento = "S";
	public String msgStatus = "";
	boolean geraTransferencia = false;
	final BigDecimal empresa5 = new BigDecimal("5"); // Extrema
	final BigDecimal empresa1 = new BigDecimal("1"); // TDB
	final BigDecimal empresa6 = new BigDecimal("6"); // Loja
	JapeWrapper cabecalhoDAO = JapeFactory.dao("CabecalhoNota");
	JapeWrapper itemDAO = JapeFactory.dao("ItemNota");
	JapeWrapper logDAO = JapeFactory.dao("AD_LOGSPLIT");
	BigDecimal saldoDesconto = BigDecimal.ZERO;
	String mensagemRetorno = "Opa!!!";
	String abortaPedidoPorFaltaDeEstoque = "N";
	BigDecimal codEmpOriginal = null;

	
	@Override
	public void doAction(ContextoAcao arg0) throws Exception {

		String nuNotaString = arg0.getParam("NUNOTA") + "";
		final BigDecimal nuNota = new BigDecimal(nuNotaString);
		
		System.out.println("=============================== iniciando job split ===============================");

		SessionHandle hnd = null;

		try {
			hnd = JapeSession.open();
			hnd.execWithTX(new JapeSession.TXBlock() {
				public void doWithTx() throws Exception {
					
					BigDecimal topPreSplit = TdbHelper.buscaTopPreSplit();

					String filtroPedidosAptos = "PENDENTE = 'S' " + "AND CODVEND = 14 "
							+ "/*AND CODCIDDESTINO NOT IN (4798, 2475)*/ " + "AND CODTIPOPER = " + topPreSplit
							+ "AND NOT EXISTS (SELECT 1 FROM TGFVAR V WHERE TGFCAB.NUNOTA = V.NUNOTAORIG) "
							+ "AND DTNEG >= '02/02/2022' AND CODEMP = 1";

					filtroPedidosAptos = "NUNOTA = " + nuNota;

					Collection<DynamicVO> pedidosValidos = cabecalhoDAO.find(filtroPedidosAptos);
					for (DynamicVO pedidoVO : pedidosValidos) {
						
						System.out.println("[Sattva] - Entrando no for - Pedido: " + nuNota);
						
						BigDecimal vlrDescTotCab = pedidoVO.asBigDecimal("VLRDESCTOT");
						BigDecimal vlrDescTotItem = pedidoVO.asBigDecimal("VLRDESCTOTITEM");
//						BigDecimal vlrDescTot = vlrDescTotItem.add(vlrDescTotCab);
						
						BigDecimal vlrDescTot = vlrDescTotCab;
						
						if (vlrDescTot.doubleValue() == 0) {
							vlrDescTot = vlrDescTotItem;
						}
//						BigDecimal vlrFrete = pedidoVO.asBigDecimal("VLRFRETE");
						BigDecimal vlrNota = pedidoVO.asBigDecimal("VLRNOTA").add(pedidoVO.asBigDecimal("VLRDESCTOT"));
						
						codEmpOriginal = pedidoVO.asBigDecimal("CODEMP");
						
						log = "";
						regraPrioridade = "";
						geraTransferencia = false;
						BigDecimal nuNotaPreSplit = pedidoVO.asBigDecimal("NUNOTA");
						BigDecimal codCidadeDestino = pedidoVO.asBigDecimal("CODCIDDESTINO");
						String nomeCidadeDestino = buscaCidadeDestino(codCidadeDestino);
						Collection<Transferencia> itensTransferencia = new ArrayList<Transferencia>();
						Collection<Split> splitPedidos = new ArrayList<Split>();
						
						boolean pedidoJaFaturado = verificaPedidoFaturado(nuNotaPreSplit);
						
						if (pedidoJaFaturado) {
							System.out.println("[Sattva] - Esse pedido já foi faturado.");
							log = "Esse pedido ja foi faturado. Verificar documentos relacionados";
							statusProcessamento = "E";
							logDAO.create()
				        	.set("DESCRICAO", log.toCharArray())
				        	.set("NUNOTAORIG", pedidoVO.asBigDecimal("NUNOTA"))
				        	.set("DHINCLUSAO", TimeUtils.getNow())
				        	.set("STATUSPROCESSAMENTO", statusProcessamento)
				        	.set("MSGSTATUS", "Pedido já faturado")
				        	.save();
							
							continue;
							
						}
						
						System.out.println("[Sattva] - Nro Unico do Pre Split: " + nuNotaPreSplit);
						
						if (codCidadeDestino != null && "4798-2475".indexOf(codCidadeDestino.toString()) > -1) {
							log += "Regra Prioridade: Empresa 1, Empresa 6 e Empresa 5";
							regraPrioridade = "165";
						} else {
							if (codCidadeDestino == null && pedidoVO.asString("BH_METODO").equalsIgnoreCase("FROTA INTERNA STANDARD")) {
								log += "Regra Prioridade: Empresa 1, Empresa 6 e Empresa 5";
								regraPrioridade = "165";
							} else {
								log += "Regra Prioridade: Empresa 5, Empresa 1 e Empresa 6";
								regraPrioridade = "516";
							}
						}

						Collection<DynamicVO> itensPreSplit = itemDAO.find("NUNOTA = ?", nuNotaPreSplit);
						log += "\nNroUnico Pre Split: " + nuNotaPreSplit;
						log += "\nBH_CODEMKT: " + pedidoVO.asString("BH_CODMKT");
						log += "\nQuantidade de Itens: " + itensPreSplit.size();
						log += "\nCidade de Destino:" + codCidadeDestino + "-" + nomeCidadeDestino;
						System.out.println("[Sattva] - Chegou aqui...");
						
						doSplit(regraPrioridade, itensPreSplit, nuNotaPreSplit, itensTransferencia, splitPedidos);
						
						/* Bruno tinha me passado essa regra no dia 07/12, porem Andressa disse que essa regra não existe. Trecho comentado no dia 08/12						
						if ("S".equals(abortaPedidoPorFaltaDeEstoque)) {
							log += "\nPedido foi desconsiderado por falta de estoque de itens.";
							abortaPedidoPorFaltaDeEstoque = "N";
							statusProcessamento = "E";
							logDAO.create().set("DESCRICAO", log.toCharArray())
									.set("NUNOTAORIG", pedidoVO.asBigDecimal("NUNOTA"))
									.set("DHINCLUSAO", TimeUtils.getNow())
									.set("STATUSPROCESSAMENTO", statusProcessamento)
									.save();
							continue;
						}
						*/
						
						if(geraTransferencia) {
							System.out.println("[Sattva] - Gerando Transferencias - Inicio");
                    		try { 
                    			Map<String, BigDecimal> nroUnicoTransf = TdbHelper.transfereSaldo6x1(itensTransferencia, nuNotaPreSplit);
                    			log += "\n\nNro.Unico.Transferencia Saida..: " + nroUnicoTransf.get("NUNOTATRANSFSAIDA");
                        		log += "\nNro.Unico.Transferencia Entrada: " + nroUnicoTransf.get("NUNOTATRANSFENTRADA")+"\n";
                        		
                        		System.out.println("[Sattva] - Recalculando transferencia");
                        		TdbHelper.recalculaImpostoEFinanceiro(nroUnicoTransf.get("NUNOTATRANSFSAIDA"));
                        		TdbHelper.recalculaImpostoEFinanceiro(nroUnicoTransf.get("NUNOTATRANSFENTRADA"));
                        		System.out.println("[Sattva] - Fim - Recalculando transferencia");
                        		
							} catch (Exception e) {
								statusProcessamento = "E";
								msgStatus = e.toString();
								logDAO.create()
		                    	.set("DESCRICAO", log.toCharArray())
		                    	.set("NUNOTAORIG", pedidoVO.asBigDecimal("NUNOTA"))
		                    	.set("DHINCLUSAO", TimeUtils.getNow())
		                    	.set("STATUSPROCESSAMENTO", statusProcessamento)
		                    	.set("MSGSTATUS", msgStatus)
		                    	.save();
								
							}
                    		System.out.println("[Sattva] - Gerando Transferencias - Fim");
                    	}
						
						System.out.println("[Sattva] - Agrupando Split por Empresa");
						Collection<Split> pedidosSplit = TdbHelper.agrupaSplitPorEmpresa(splitPedidos);
						Map<BigDecimal, BigDecimal> listaNroUnicoEmpresa = new HashMap<BigDecimal, BigDecimal>();
						
						System.out.println("[Sattva] - Gerando Lançamentos Splits - Inicio");
						listaNroUnicoEmpresa = TdbHelper.geraLancamentosSplit(pedidoVO, pedidosSplit);							
						System.out.println("[Sattva] - Gerando Lançamentos Splits - Fim");
						
						System.out.println("[Sattva] - Imprimindo Log - Inicio");
                    	imprimeSplitFinal(pedidosSplit, listaNroUnicoEmpresa);
                    	System.out.println("[Sattva] - Imprimindo Log - Fim");
                    	
                    	if (vlrDescTot.doubleValue() > 0) {
                    		System.out.println("[Sattva] - Atualizando desconto dos lançamentos splitados - Inicio");
                    		atualizaDesconto(listaNroUnicoEmpresa, vlrNota, vlrDescTot, saldoDesconto);                    		
                    		System.out.println("[Sattva] - Atualizando desconto dos lançamentos splitados - Fim");
                    	}

                    	refazValoresTotaisPedido(listaNroUnicoEmpresa);
                    	
                    	System.out.println("[Sattva] - Fazendo ligações na TGFVAR - Inicio");
                    	TdbHelper.vinculaTgfvar(listaNroUnicoEmpresa, pedidoVO.asBigDecimal("NUNOTA"));
                    	System.out.println("[Sattva] - Fazendo ligações na TGFVAR - Fim");
                    	
                    	BigDecimal topPosSplit = TdbHelper.buscaTopPreSplit();
                    	System.out.println("[Sattva] - Atualizando top pedido origem");
                    	cabecalhoDAO.prepareToUpdate(pedidoVO)
                    	.set("CODTIPOPER", topPosSplit)
                    	.set("DHTIPOPER", TdbHelper.getDhTipOper(topPosSplit))
                    	.update();
                    	System.out.println("[Sattva] - Fim Atualização");
                    	
//                    	CentralNotasUtils.confirmarNota(pedidoVO.asBigDecimal("NUNOTA"));
		
						logDAO.create()
			        	.set("DESCRICAO", log.toCharArray())
			        	.set("NUNOTAORIG", pedidoVO.asBigDecimal("NUNOTA"))
			        	.set("DHINCLUSAO", TimeUtils.getNow())
			        	.set("STATUSPROCESSAMENTO", statusProcessamento)
			        	.save();

						abortaPedidoPorFaltaDeEstoque = "N";
						
					}
					
					System.out.println("[Sattva] - Proximo pedido... alterando variavel para zero.");
					saldoDesconto = BigDecimal.ZERO;

				}
				
				private void refazValoresTotaisPedido(Map<BigDecimal, BigDecimal> listaNroUnicoEmpresa) throws Exception {
					System.out.println("[Sattva] - Refazendo Valores: " + listaNroUnicoEmpresa);
					
					for (Map.Entry<BigDecimal, BigDecimal> nuNotaEmp : listaNroUnicoEmpresa.entrySet()) {
						System.out.println("[Sattva] - Recalculando imposto e financeiro");
            			TdbHelper.recalculaImpostoEFinanceiro(nuNotaEmp.getValue());
            			System.out.println("[Sattva] - Recalculo finalizado");
					}
				
				}

				private void atualizaDesconto(Map<BigDecimal, BigDecimal> listaNroUnicoEmpresa, BigDecimal vlrNota, BigDecimal vlrDescTot, BigDecimal saldoDesconto) throws Exception {
					System.out.println("[Sattva] - Lista de Nro.Unico p/ desconto: " + listaNroUnicoEmpresa);
					System.out.println("[Sattva] - Saldo Desconto: " + saldoDesconto);
					
					BigDecimal vlrDesconto = BigDecimal.ZERO;	

					if (listaNroUnicoEmpresa.size() == 1) {
						vlrDesconto = vlrDescTot;
					} else {
						
						for (Map.Entry<BigDecimal, BigDecimal> nuNotaEmp : listaNroUnicoEmpresa.entrySet()) {
							
							if (vlrDesconto.doubleValue() > 0) {
								
								System.out.println("[Sattva] - Saldo do desconto > 0... utilizando a regra de acumulado...");
								System.out.println("[Sattva] - vlrDescTot: " + vlrDescTot + ", saldoDesconto: " + saldoDesconto);
								
								vlrDesconto = vlrDescTot.subtract(vlrDesconto);
								
								cabecalhoDAO.prepareToUpdateByPK(nuNotaEmp.getValue()).set("VLRDESCTOT", vlrDesconto).update();
								
								/*
								System.out.println("[Sattva] - Recalculando imposto e financeiro");
		            			TdbHelper.recalculaImpostoEFinanceiro(nuNotaEmp.getValue());
		            			System.out.println("[Sattva] - Recalculo finalizado");
		            			*/
		            			
							} else {
								
								EntityFacade dwf = EntityFacadeFactory.getDWFFacade();
								NativeSql qrySomaItens = new NativeSql(dwf.getJdbcWrapper());
								qrySomaItens.appendSql("SELECT "
										+ "SUM(ITE.VLRTOT) AS TOTAL"
										+ ", CAB.VLRFRETE "
										+ ", SUM(ITE.VLRTOT) * VCAB.INDITENS AS TOTAL_PROP "
										+ "FROM TGFITE ITE "
										+ "JOIN TGFCAB CAB ON ITE.NUNOTA = CAB.NUNOTA "
										+ "JOIN VGFCAB VCAB ON CAB.NUNOTA = VCAB.NUNOTA "
										+ "WHERE ITE.NUNOTA = :NUNOTA AND ITE.USOPROD <> 'D' "
										+ "GROUP BY CAB.VLRFRETE, VCAB.INDITENS");
								qrySomaItens.setNamedParameter("NUNOTA", nuNotaEmp.getValue());
								
								ResultSet rs = qrySomaItens.executeQuery();
								if (rs.next()) {
									
									if (rs.getBigDecimal(1).doubleValue() > vlrNota.doubleValue()) {
										// entrando aqui significa que o desconto ja foi rateado para os itens
										vlrDesconto = BigDecimalUtil.getRounded(((rs.getBigDecimal(1).add(rs.getBigDecimal(2))).multiply(vlrDescTot)).divide((vlrNota.add(vlrDescTot)),10,5),2);
										
										
										System.out.println("[Sattva] - #Memoria Calculo Desconto: Total Itens: " 
												+ rs.getBigDecimal(1) 
												+ ", Vlr Frete: " 
												+ rs.getBigDecimal(2)
												+ ", Desconto Total: " + vlrDescTot + ", Valor do Pedido: " +  vlrNota.add(vlrDescTot));
										System.out.println("(" + rs.getBigDecimal(1) + "+" + rs.getBigDecimal(2) + ") * " + vlrDescTot + " / (" + vlrNota + " + " + vlrDescTot + ")");
										System.out.println("[Sattva] - Valor do desconto proporcional para a nota " + nuNotaEmp.getValue() + ": " + vlrDesconto);
										
										
										
									} else {
										vlrDesconto = BigDecimalUtil.getRounded(((rs.getBigDecimal(1).add(rs.getBigDecimal(2))).multiply(vlrDescTot)).divide(vlrNota,10,5),2);
										
										System.out.println("[Sattva] - Memoria Calculo Desconto: Total Itens: " 
												+ rs.getBigDecimal(1) 
												+ ", Vlr Frete: " 
												+ rs.getBigDecimal(2)
												+ ", Desconto Total: " + vlrDescTot + ", Valor do Pedido: " +  vlrNota);
										System.out.println("[Sattva] - Valor do desconto proporcional para a nota " + nuNotaEmp.getValue() + ": " + vlrDesconto);
										
									}
									
									cabecalhoDAO.prepareToUpdateByPK(nuNotaEmp.getValue()).set("VLRDESCTOT", vlrDesconto).update();
									
									System.out.println("[Sattva] - Atualizado desconto");
									
									/*
									System.out.println("[Sattva] - Recalculando imposto e financeiro");
			            			TdbHelper.recalculaImpostoEFinanceiro(nuNotaEmp.getValue());
			            			System.out.println("[Sattva] - Recalculo finalizado");
			            			*/
									
								}
							}
							
						}
						
					}
					
				}

				private void atualizaDesconto(Map<BigDecimal, BigDecimal> listaNroUnicoEmpresa, BigDecimal vlrNota, BigDecimal vlrDescTot) throws Exception {
					
					for (Map.Entry<BigDecimal, BigDecimal> nuNotaEmp : listaNroUnicoEmpresa.entrySet()) {
						
						EntityFacade dwf = EntityFacadeFactory.getDWFFacade();
						NativeSql qrySomaItens = new NativeSql(dwf.getJdbcWrapper());
						qrySomaItens.appendSql("SELECT "
								+ "SUM(ITE.VLRTOT) AS TOTAL"
								+ ", CAB.VLRFRETE "
								+ ", SUM(ITE.VLRTOT) * VCAB.INDITENS AS TOTAL_PROP "
								+ "FROM TGFITE ITE "
								+ "JOIN TGFCAB CAB ON ITE.NUNOTA = CAB.NUNOTA "
								+ "JOIN VGFCAB VCAB ON CAB.NUNOTA = VCAB.NUNOTA "
								+ "WHERE ITE.NUNOTA = :NUNOTA AND ITE.USOPROD <> 'D' "
								+ "GROUP BY CAB.VLRFRETE, VCAB.INDITENS");
						qrySomaItens.setNamedParameter("NUNOTA", nuNotaEmp.getValue());
						
						ResultSet rs = qrySomaItens.executeQuery();
						if (rs.next()) {
							BigDecimal vlrDesconto = ((rs.getBigDecimal(1).add(rs.getBigDecimal(2))).multiply(vlrDescTot)).divide(vlrNota,10,5);
							System.out.println("[Sattva] - Memoria Calculo Desconto: Total Itens: " + rs.getBigDecimal(1) + ", Vlr Frete: " + rs.getBigDecimal(2)
									+ ", Desconto Total: " + vlrDescTot + ", Valor do Pedido: " +  vlrNota);
							System.out.println("[Sattva] - Valor do desconto proporcional para a nota " + nuNotaEmp.getValue() + ": " + vlrDesconto);
							
							cabecalhoDAO.prepareToUpdateByPK(nuNotaEmp.getValue()).set("VLRDESCTOT", vlrDesconto).update();
							
							System.out.println("[Sattva] - Atualizado desconto");
							
							System.out.println("[Sattva] - Recalculando imposto e financeiro");
                			TdbHelper.recalculaImpostoEFinanceiro(nuNotaEmp.getValue());
                			System.out.println("[Sattva] - Recalculo finalizado");
							
						}
						
					}
					
					
				}

				private boolean verificaPedidoFaturado(BigDecimal nuNotaPreSplit) throws Exception {
					JapeWrapper tgfvarDAO = JapeFactory.dao("CompraVendavariosPedido");
					DynamicVO tgfvarVO = tgfvarDAO.findOne("NUNOTAORIG = ? AND NUNOTAORIG <> NUNOTA", nuNotaPreSplit);
					
					if (tgfvarVO == null) {
						return false;
					}
					
					return true;
				}

				private void imprimeSplitFinal(Collection<Split> pedidosSplit, Map<BigDecimal, BigDecimal> listaNroUnicoEmpresa) throws Exception {
					for (Split pedido : pedidosSplit) {
						for (Map.Entry<BigDecimal, BigDecimal> nuNotaEmp : listaNroUnicoEmpresa.entrySet()) {
							
							if (nuNotaEmp.getKey().intValue() == 1 && pedido.codEmp.intValue() == 1) {
								log += "\n" + "[NUNOTA: " + nuNotaEmp.getValue() + "] " + pedido.toString();
							}
							if (nuNotaEmp.getKey().intValue() == 5 && pedido.codEmp.intValue() == 5) {
								log += "\n" + "[NUNOTA: " + nuNotaEmp.getValue() + "] " + pedido.toString();
							}
						}
					}
				}

				private void doSplit(String regraPrioridade, Collection<DynamicVO> itensPreSplit, BigDecimal nuNotaPreSplit, Collection<Transferencia> itensTransferencia, Collection<Split> splitPedidos) throws Exception {
					JapeWrapper produtoDAO = JapeFactory.dao("Produto");
					for (DynamicVO itemPreSplitVO : itensPreSplit) {
						BigDecimal qtdNeg = itemPreSplitVO.asBigDecimal("QTDNEG");
						BigDecimal codProd = itemPreSplitVO.asBigDecimal("CODPROD");
						String nomeProduto = itemPreSplitVO.asString("Produto.DESCRPROD");
			    		BigDecimal saldo = qtdNeg;
			    		BigDecimal vlrUnit = itemPreSplitVO.asBigDecimal("VLRUNIT");
			    		
			    		DynamicVO produtoVO = produtoDAO.findOne("CODPROD = ?", codProd);
			    		if("D".equals(produtoVO.asString("USOPROD"))) {
			    			continue;
			    		}
			    		
			    		BigDecimal estDisponivelEmp5 = verificaSaldoEstoqueAgrupando(empresa5, codProd, qtdNeg, codEmpOriginal);
			    		BigDecimal estDisponivelEmp1 = verificaSaldoEstoqueAgrupando(empresa1, codProd, qtdNeg, codEmpOriginal);
			    		BigDecimal estDisponivelEmp6 = verificaSaldoEstoqueAgrupando(empresa6, codProd, qtdNeg, codEmpOriginal);
			    		
			    		if ((estDisponivelEmp5.add(estDisponivelEmp1).add(estDisponivelEmp6)).doubleValue() < qtdNeg.doubleValue()) {
			    			log += "\n Quantidade do Produto: " + codProd + ", [QtdNeg: " + qtdNeg + "] é maior que a quantidade geral do estoque [" 
			    					+  (estDisponivelEmp5.add(estDisponivelEmp1).add(estDisponivelEmp6)).doubleValue() + "]";
			    			
			    			splitPedidos.add(new Split(empresa1, codProd, qtdNeg, vlrUnit));
			    			
			    			abortaPedidoPorFaltaDeEstoque = "S";
							continue;
			    			
			    		}
			    		
			    		if (regraPrioridade.equals("516")) {
			    			log += "\n\nProduto: " + codProd + ", QtdNeg: " + qtdNeg 
				    				+ "\nDisponibilidade(5): " + estDisponivelEmp5 
				    				+ ", Disponibilidade(1): " + estDisponivelEmp1
				    				+ ", Disponibilidade(6): " + estDisponivelEmp6;
			    		} else {
			    			log += "\n\nProduto: " + codProd + ", QtdNeg: " + qtdNeg 
				    				+ "\nDisponibilidade(1): " + estDisponivelEmp1
				    				+ ", Disponibilidade(6): " + estDisponivelEmp6
				    				+ ", Disponibilidade(5): " + estDisponivelEmp5;
			    		}
			    		
			    		if (regraPrioridade.equals("165")) {
			    			BigDecimal saldoPendente = verificaDisponibilidade(empresa1, saldo, estDisponivelEmp1, false, itensTransferencia, codProd, splitPedidos, vlrUnit);
			    			saldoPendente = verificaDisponibilidade(empresa6, saldoPendente, estDisponivelEmp6, true, itensTransferencia, codProd, splitPedidos, vlrUnit);
			    			saldoPendente = verificaDisponibilidade(empresa5, saldoPendente, estDisponivelEmp5, false, itensTransferencia, codProd, splitPedidos, vlrUnit);
			    		} 

			    		if (regraPrioridade.equals("516")) {
			    			BigDecimal saldoPendente = verificaDisponibilidade(empresa5, saldo, estDisponivelEmp5, false, itensTransferencia, codProd, splitPedidos, vlrUnit);
			    			saldoPendente = verificaDisponibilidade(empresa1, saldoPendente, estDisponivelEmp1, false, itensTransferencia, codProd, splitPedidos, vlrUnit);
			    			saldoPendente = verificaDisponibilidade(empresa6, saldoPendente, estDisponivelEmp6, true, itensTransferencia, codProd, splitPedidos, vlrUnit);
			    		}
						
					}
					
				}

				private BigDecimal verificaDisponibilidade(BigDecimal empresa, BigDecimal saldo, BigDecimal estDisponivel, boolean realizaTransferencia, Collection<Transferencia> itensTransferencia, BigDecimal codProd, Collection<Split> splitPedidos, BigDecimal vlrUnit) {
					//Estoque Total disponivel
					if (saldo.doubleValue() > 0 && estDisponivel.doubleValue() > 0 && estDisponivel.doubleValue() >= saldo.doubleValue()) {
						if (realizaTransferencia) {
							geraTransferencia = true;
							itensTransferencia.add(new Transferencia(codProd, saldo));
							splitPedidos.add(new Split(empresa1, codProd, saldo, vlrUnit));
							log += "\nEstoque na empresa " + empresa + " com disponibilidade total do saldo";
							log += "\nFazendo transferencia de " + saldo + " da empresa 6 para 1";
						} else {
							splitPedidos.add(new Split(empresa, codProd, saldo, vlrUnit));
							log += "\nEstoque na empresa " + empresa + " com disponibilidade total do saldo";
						}
						return BigDecimal.ZERO;
					}
					
					//Estoque Parcial disponivel
					if (saldo.doubleValue() > 0 && estDisponivel.doubleValue() > 0 && estDisponivel.doubleValue() < saldo.doubleValue()) {
						if (realizaTransferencia) {
							geraTransferencia = true;
							itensTransferencia.add(new Transferencia(codProd, estDisponivel));
							splitPedidos.add(new Split(empresa1, codProd, estDisponivel, vlrUnit));
							log += "\nEstoque na empresa " + empresa + " com disponibilidade parcial. Saldo pendente: " + saldo.subtract(estDisponivel);
							log += "\nFazendo transferencia de " + estDisponivel + " da empresa 6 para 1";
						} else {
							splitPedidos.add(new Split(empresa, codProd, estDisponivel, vlrUnit));
							log += "\nEstoque na empresa: " + empresa + " com disponibilidade parcial. Saldo pendente: " + saldo.subtract(estDisponivel);
						}
					}

					return saldo.subtract(estDisponivel);
				}

				private BigDecimal verificaSaldoEstoqueAgrupando(BigDecimal codEmp, BigDecimal codProd, BigDecimal qtdNeg, BigDecimal codEmpOriginal) throws Exception {
					EntityFacade dwf = EntityFacadeFactory.getDWFFacade();
					NativeSql sqlEstoqueDisponivel = new NativeSql(dwf.getJdbcWrapper());
					sqlEstoqueDisponivel.loadSql(getClass(), "qryEstoqueDisponivel.sql");
					sqlEstoqueDisponivel.setNamedParameter("CODEMP", codEmp);
					sqlEstoqueDisponivel.setNamedParameter("CODPROD", codProd);
					sqlEstoqueDisponivel.setNamedParameter("QTDNEG", qtdNeg);
					sqlEstoqueDisponivel.setNamedParameter("CODEMPPEDIDO", codEmpOriginal);
					
					ResultSet rs = sqlEstoqueDisponivel.executeQuery();
					if (rs.next()) {
						return rs.getBigDecimal(1);
					} else {
						return BigDecimal.ZERO;
					}
				}
			});
		} catch (Exception e) {
			System.out.println("[SattvaError1: " + e.toString());
			mensagemRetorno = "Erro: " + e.toString();
			e.printStackTrace();
		} 
			finally {
			JapeSession.close(hnd);
			System.out.println("=============================== Finalizando job split ===============================");
		}
		
		arg0.setMensagemRetorno(mensagemRetorno);
	}

	private String buscaCidadeDestino(BigDecimal codCidadeDestino) throws Exception {
		if (codCidadeDestino == null) {
			return "";
		}
		JapeWrapper cidadeDAO = JapeFactory.dao("Cidade");
		DynamicVO cidadeVO = cidadeDAO.findOne("CODCID = ?", codCidadeDestino);
		return cidadeVO.asString("NOMECID");
	}

}
