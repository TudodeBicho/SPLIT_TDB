package br.com.sattva.tdb.acoes;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

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
import br.com.sattva.tdb.utils.CentralNotasUtils;
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

	@Override
	public void doAction(ContextoAcao arg0) throws Exception {

		String nuNotaString = arg0.getParam("NUNOTA") + "";
		final BigDecimal nuNota = new BigDecimal(nuNotaString);

		SessionHandle hnd = null;

		try {
			hnd = JapeSession.open();
			hnd.execWithTX(new JapeSession.TXBlock() {
				public void doWithTx() throws Exception {

					String filtroPedidosAptos = "PENDENTE = 'S' " + "AND CODVEND = 14 "
							+ "/*AND CODCIDDESTINO NOT IN (4798, 2475)*/ " + "AND CODTIPOPER = 3102 "
							+ "AND NOT EXISTS (SELECT 1 FROM TGFVAR V WHERE TGFCAB.NUNOTA = V.NUNOTAORIG) "
							+ "AND DTNEG >= '02/02/2022' AND CODEMP = 1";

					filtroPedidosAptos = "NUNOTA = " + nuNota;

					Collection<DynamicVO> pedidosValidos = cabecalhoDAO.find(filtroPedidosAptos);
					for (DynamicVO pedidoVO : pedidosValidos) {
						log = "";
						regraPrioridade = "";
						geraTransferencia = false;
						BigDecimal nuNotaPreSplit = pedidoVO.asBigDecimal("NUNOTA");
						BigDecimal codCidadeDestino = pedidoVO.asBigDecimal("CODCIDDESTINO");
						String nomeCidadeDestino = buscaCidadeDestino(codCidadeDestino);
						Collection<Transferencia> itensTransferencia = new ArrayList<Transferencia>();
						Collection<Split> splitPedidos = new ArrayList<Split>();

						if (codCidadeDestino != null && "4798-2475".indexOf(codCidadeDestino.toString()) > -1) {
							log += "Regra Prioridade: Empresa 1, Empresa 6 e Empresa 5";
							regraPrioridade = "165";
						} else {
							log += "Regra Prioridade: Empresa 5, Empresa 1 e Empresa 6";
							regraPrioridade = "516";
						}

						Collection<DynamicVO> itensPreSplit = itemDAO.find("NUNOTA = ?", nuNotaPreSplit);
						log += "\nNroUnico Pre Split: " + nuNotaPreSplit;
						log += "\nQuantidade de Itens: " + itensPreSplit.size();
						log += "\nCidade de Destino:" + codCidadeDestino + "-" + nomeCidadeDestino;
						System.out.println("[Sattva] - Chegou aqui...");
						
						doSplit(regraPrioridade, itensPreSplit, nuNotaPreSplit, itensTransferencia, splitPedidos);
						
						if(geraTransferencia) {
                    		try {
                    			Map<String, BigDecimal> nroUnicoTransf = TdbHelper.transfereSaldo6x1(itensTransferencia);
                    			log += "\nNro.Unico.Transferencia Saida..: " + nroUnicoTransf.get("NUNOTATRANSFSAIDA");
                        		log += "\nNro.Unico.Transferencia Entrada: " + nroUnicoTransf.get("NUNOTATRANSFENTRADA");
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
                    	}
						
						Collection<Split> pedidosSplit = TdbHelper.agrupaSplitPorEmpresa(splitPedidos);
                    	Map<BigDecimal, BigDecimal> listaNroUnicoEmpresa = TdbHelper.geraLancamentosSplit(pedidoVO, pedidosSplit);
                    	
                    	imprimeSplitFinal(pedidosSplit, listaNroUnicoEmpresa);
                    	
                    	cabecalhoDAO.prepareToUpdate(pedidoVO)
                    	.set("CODTIPOPER", new BigDecimal("3131"))
                    	.set("DHTIPOPER", TdbHelper.getDhTipOper(new BigDecimal("3131")))
                    	.update();
                    	
                    	CentralNotasUtils.confirmarNota(pedidoVO.asBigDecimal("NUNOTA"));
		
						logDAO.create()
			        	.set("DESCRICAO", log.toCharArray())
			        	.set("NUNOTAORIG", pedidoVO.asBigDecimal("NUNOTA"))
			        	.set("DHINCLUSAO", TimeUtils.getNow())
			        	.set("STATUSPROCESSAMENTO", statusProcessamento)
			        	.save();

						
					}

				}

				private void imprimeSplitFinal(Collection<Split> pedidosSplit, Map<BigDecimal, BigDecimal> listaNroUnicoEmpresa) {
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
			    		
			    		DynamicVO produtoVO = produtoDAO.findOne("CODPROD = ?", codProd);
			    		if("D".equals(produtoVO.asString("USOPROD"))) {
			    			continue;
			    		}
			    		
			    		BigDecimal estDisponivelEmp5 = verificaSaldoEstoqueAgrupando(empresa5, codProd);
			    		BigDecimal estDisponivelEmp1 = verificaSaldoEstoqueAgrupando(empresa1, codProd);
			    		BigDecimal estDisponivelEmp6 = verificaSaldoEstoqueAgrupando(empresa6, codProd);
			    		
			    		if ((estDisponivelEmp5.add(estDisponivelEmp1).add(estDisponivelEmp6)).doubleValue() < qtdNeg.doubleValue()) {
			    			log += "\n Quantidade do Produto: " + codProd + ", [QtdNeg: " + qtdNeg + "] é maior que a quantidade geral do estoque [" 
			    					+  (estDisponivelEmp5.add(estDisponivelEmp1).add(estDisponivelEmp6)).doubleValue() + "]";
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
			    			BigDecimal saldoPendente = verificaDisponibilidade(empresa1, saldo, estDisponivelEmp1, false, itensTransferencia, codProd, splitPedidos);
			    			saldoPendente = verificaDisponibilidade(empresa6, saldoPendente, estDisponivelEmp6, true, itensTransferencia, codProd, splitPedidos);
			    			saldoPendente = verificaDisponibilidade(empresa5, saldoPendente, estDisponivelEmp5, false, itensTransferencia, codProd, splitPedidos);
			    		} 

			    		if (regraPrioridade.equals("516")) {
			    			BigDecimal saldoPendente = verificaDisponibilidade(empresa5, saldo, estDisponivelEmp5, false, itensTransferencia, codProd, splitPedidos);
			    			saldoPendente = verificaDisponibilidade(empresa1, saldoPendente, estDisponivelEmp1, false, itensTransferencia, codProd, splitPedidos);
			    			saldoPendente = verificaDisponibilidade(empresa6, saldoPendente, estDisponivelEmp6, true, itensTransferencia, codProd, splitPedidos);
			    		}
						
					}
					
				}

				private BigDecimal verificaDisponibilidade(BigDecimal empresa, BigDecimal saldo, BigDecimal estDisponivel, boolean realizaTransferencia, Collection<Transferencia> itensTransferencia, BigDecimal codProd, Collection<Split> splitPedidos) {
					//Estoque Total disponivel
					if (saldo.doubleValue() > 0 && estDisponivel.doubleValue() > 0 && estDisponivel.doubleValue() >= saldo.doubleValue()) {
						if (realizaTransferencia) {
							geraTransferencia = true;
							itensTransferencia.add(new Transferencia(codProd, saldo));
							splitPedidos.add(new Split(empresa1, codProd, saldo));
							log += "\nEstoque na empresa " + empresa + " com disponibilidade total do saldo";
							log += "\nFazendo transferencia de " + saldo + " da empresa 6 para 1";
						} else {
							splitPedidos.add(new Split(empresa, codProd, saldo));
							log += "\nEstoque na empresa " + empresa + " com disponibilidade total do saldo";
						}
						return BigDecimal.ZERO;
					}
					
					//Estoque Parcial disponivel
					if (saldo.doubleValue() > 0 && estDisponivel.doubleValue() > 0 && estDisponivel.doubleValue() < saldo.doubleValue()) {
						if (realizaTransferencia) {
							geraTransferencia = true;
							itensTransferencia.add(new Transferencia(codProd, estDisponivel));
							splitPedidos.add(new Split(empresa1, codProd, estDisponivel));
							log += "\nEstoque na empresa " + empresa + " com disponibilidade parcial. Saldo pendente: " + saldo.subtract(estDisponivel);
							log += "\nFazendo transferencia de " + estDisponivel + " da empresa 6 para 1";
						} else {
							splitPedidos.add(new Split(empresa, codProd, estDisponivel));
							log += "\nEstoque na empresa: " + empresa + " com disponibilidade parcial. Saldo pendente: " + saldo.subtract(estDisponivel);
						}
					}

					return saldo.subtract(estDisponivel);
				}

				private Object disponibilidaDeAtendimento(BigDecimal saldo, BigDecimal estDisponivelEmp1) {
					// TODO Auto-generated method stub
					return null;
				}

				private BigDecimal verificaSaldoEstoqueAgrupando(BigDecimal codEmp, BigDecimal codProd) throws Exception {
					EntityFacade dwf = EntityFacadeFactory.getDWFFacade();
					NativeSql sqlEstoqueDisponivel = new NativeSql(dwf.getJdbcWrapper());
					sqlEstoqueDisponivel.loadSql(getClass(), "qryEstoqueDisponivel.sql");
					sqlEstoqueDisponivel.setNamedParameter("CODEMP", codEmp);
					sqlEstoqueDisponivel.setNamedParameter("CODPROD", codProd);
					
					ResultSet rs = sqlEstoqueDisponivel.executeQuery();
					if (rs.next()) {
						return rs.getBigDecimal(1);
					} else {
						return BigDecimal.ZERO;
					}
				}
			});
		}

//		 catch (Exception e) {
//			System.out.println("[SattvaError: " + e.toString());
//			arg0.setMensagemRetorno("Erro: " + e.toString());
//		} 
			finally {
			JapeSession.close(hnd);
		}
		arg0.setMensagemRetorno("Opa!");
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
