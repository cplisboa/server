import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.StringTokenizer;

public class Grandeza {
	private String code;
	private float vazao;
	private String corrente;
	private float volume;
	private String pluviometria;
	private float nivel;
	private Logger logger;

	/** Recebe código do poço que terá dados inseridos */;
	public Grandeza (String pocoCode, Logger logger) {
		this.code = pocoCode;
		this.logger = logger;		
	}
	
	/**
	 * Faz o parser da linha de grandezas, baseado na lista de grandezas recebidas
	 * @param granList
	 * @param periodoGrandezas Tempo entre medidas em SEGUNDOS!
	 * @param linha
	 * @param lastHour
	 * @param lastNivel
	 */
	public void parseGrandeza(ArrayList<Integer> granList, int periodoGrandezas, String linha, String lastHour, String lastNivel) {
		//linha = "1.20,4.87,0.00"; //Linha para teste, remover quando usando o Server Real
		// 8,2 e 9 - 8 volume, 2 corrente e 9 pluviometria
		logger.log("Dados de Grandezas: "+linha);		
		StringTokenizer strToken = new StringTokenizer(linha, ",");
				
		for(int i=0; i<granList.size(); i++) {
			//Recuperando grandeza a ser inserida
			String value = strToken.nextToken().trim();
			switch (granList.get(i)) {	
				case 2:
					logger.log("Corrente: "+value);
					corrente=value;
					break;
				case 8:
					logger.log("Volume: "+value);
					volume=Float.parseFloat(value);
					try {
						vazao = calcVazao(volume, periodoGrandezas);
						logger.log("Vazao Calculada: "+vazao);
					} catch (Exception e) {
						logger.log("Erro calculando a vazao. "+e.getMessage());
						e.printStackTrace();
					}
					break;
				case 9:
					logger.log("Pluviometria: "+value);
					pluviometria=value;
					break;	
				default:
					logger.log("Atenção! Grandeza desconhecida: "+granList.get(i));
			}
		}
		insertGrandeza(lastHour, lastNivel);
	}
	
	/** Recebe uma base conectada e le o ultimo dado do hidrometro */
	private float readHidrometro (Database db){
		float hidro = 0;
		String consulta = "select first 1 hidrometro,nivel from grandezas where code = '"+ code +"' order by data desc"; 
		ResultSet rs = db.execQuery(consulta);
		try {
			if(rs.next()) {
				hidro = rs.getFloat("hidrometro");
				try {
					nivel = rs.getFloat("nivel");
				} catch (Exception e) {
					logger.log("Erro convertendo nivel anterior para float." + e.getMessage());
					nivel = 0;
				}
			} else {
				logger.log("*** Primeiro dado para o poço de code = "+code+" ****");
			}
			rs.close();
		} catch (Exception e) {
			logger.log("Erro lendo dados do hidrometro");
			e.printStackTrace();
		}
		return hidro;		
	}
			
	/** Calcula vazão baseado na volume recebido e periodo (recebido em segundos) */
	private float calcVazao(float volume, int periodo) {
		float vazao = 0;
		int oneHour = 60 * 60;
		if (periodo==0) {
			vazao = 0;
			logger.log("ATENÇÃO!! PERIODO É ZERO!!! NÃO PODERIA SER!!");
		} else {
			vazao = volume * oneHour / periodo;
			logger.log("Volume: "+volume);
			logger.log("Periodo: "+periodo);
			logger.log("Vazao: "+vazao);
		}
		return vazao;
	}
	
	/** Inserindo grandeza na base de dados. Basta passar o código do poço */
	private void insertGrandeza(String lastHour, String lastNivel) {
    	Database db = null;
    	float cesp = 0;
        try {
        	logger.log("Iniciando conexão com banco de dados");
            db = new Database("SYSDBA","masterkey");	                	
        }catch (Exception e) {
        	logger.log("Problema ao conectar na base "+e.getMessage());	                	
        }    	

        float hidro = volume + readHidrometro(db);

        Cesp cespObj = new Cesp(db, logger, this.code, lastHour);
        if(Float.parseFloat(corrente) > -10) {
        	// Calcula CESP apenas se houver corrente, caso contrário CESP = 0
        	try {
				cesp = cespObj.calculaCesp(hidro, Float.parseFloat(lastNivel));
			} catch (Exception e) {
				logger.log("Exception no cálculo de CESP. "+e.getMessage());
				e.printStackTrace();
			}
        	//cesp = calculaCespOld(lastNivel);
        } else {
        	logger.log("Não existe corrente > 10A. CESP do momento é ZERO!");
        }
                
        try {
        	String sql = "insert into grandezas (code, data, vazao, corrente, volume, hidrometro, cesp, nivel, pluviometria)"
        			+ " values ('"+code+"','"+lastHour+"', "+vazao+", "+corrente+", "+volume+", "+hidro+", "+cesp+", "+lastNivel+", "+pluviometria+")";
        	logger.log(sql);
        	db.insert(sql);
        	logger.log("Grandeza inserida.");
        } catch (Exception e) {
        	logger.log("Excessao inserindo dados de grandeza. "+e.getMessage());
        }	
        db.close();
	}

	
	/** Calcula CESP a partir do volume explotado / dividido pelo rebaixamento / tempo */
	/*private float calculaCespOld(String lastNivel) {
		float cesp = 0;
		float rebaixamento = 0;
	    
	    try {
	    	float nivelFloat = Float.parseFloat(lastNivel);
	    	//Calculando rebaixamento
	    	if (nivel == 0) {
	    		//Primeira medida de nivel salva em grandezas
	    		logger.log("Nivel anterior era zero. Cesp fica em zero");
	    	} else {
	    		//Calcula rebaixament
	    		rebaixamento = nivelFloat - nivel;
	    		if (rebaixamento <=0 ) {
	    			rebaixamento = 0;
	    			cesp = 0;
	    		} else {
	    			cesp = (volume / rebaixamento) * 4; //*4 pois as medidas estão sendo feitas a cada 15 min (1/4 de hora)	
	    		}
	    		logger.log("Nivel anterior: "+nivel);
	    		logger.log("Nivel Novo: "+nivelFloat);
	    		logger.log("Rebaixamento: "+rebaixamento);
	    		logger.log("Volume: "+volume);
	    		logger.log("CESP: "+cesp);
	    	}
	    } catch(Exception e) {
	    	logger.log("Erro calculando CESP. "+e.getMessage());
	    	e.printStackTrace();
	    }	    
	    return cesp;
	}*/	
	
	public static void main (String []args) {
  		// Grandeza obj = new Grandeza();
		// .insertGrandeza("codePoco");
	}
	
}