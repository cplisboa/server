import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class Cesp {

	private Database db;
	private String code;
	private String dataFormatoFirebird;
	private Logger logger;
	private Date dataAtual;
		
	public Cesp(Database db, Logger logger, String code, String dataString) {
		this.db = db;
		this.code = code;
		this.logger = logger;
		
		logger.log("Data para cálculo do CESP: " + dataString);
		if ((dataString == null) || dataString.equals("")) {
			logger.log("Data p/ calculo do CESP veio NULL. Vamos usar medida = vazio");
			this.dataAtual = Calendar.getInstance().getTime();
		} else {
			int year = Integer.parseInt(dataString.substring(0,4));
			int month = Integer.parseInt(dataString.substring(5,7));
			int day = Integer.parseInt(dataString.substring(8,10));
			int hora = Integer.parseInt(dataString.substring(11,13))-1;
			int minuto = Integer.parseInt(dataString.substring(14,16));
			int segundo = Integer.parseInt(dataString.substring(17,19));
			this.dataFormatoFirebird = month+"-"+day+"-"+year;	
			logger.log("Hora: "+hora+":"+minuto+":"+segundo);
			logger.log("Data no formado do firebird: "+dataFormatoFirebird);
			this.dataAtual = new Date(year-1900, month-1, day, hora, minuto, segundo);
			
		}
		logger.log("Data no formato date: "+dataAtual.toString());
		
	}
	
	// vazao / tempo /rebaixamento
	/**
	 * Segundo prof. Lisboa. CESP = vazao media / Rebaixamento
	 * Vazao média = volume bombeado / tempo (em hs)
	 * @param volume
	 * @param dataAtual
	 * @param nivelAtual
	 * @return
	 * @throws Exception
	 */
	public float calculaCesp(float hidrometro, Float nivelAtual) throws Exception {
		float cesp = 0; 

		//Recupera medida inicial
		Medida medInicial = getMedidaInicial();		
		
		//Caso medInicial = null, não existem dados para o dia
		if (medInicial!= null) {			
			//Calcula diferença de tempo entre o início do bombeamento e o tempo atual (em segundos)
			float tempo = diffTempo(this.dataAtual, medInicial.getDate());
			
			//Calculando o volume bombeado com base no valor do hidrometro na medida inicial
			float volume = hidrometro - medInicial.getHidrometro();
			
			//Tempo em hs
			tempo = tempo / 60 / 60;
			logger.log("Volume: " + volume);
			logger.log("Diferença de tempo em horas: " + tempo);
			
			//Calculando Vazao Média(volume bombeado no período)
			float vazao = volume / tempo;
			logger.log("Vazao Calcudada: " + vazao);
			
			//Calcula rebaixamento a partir da medida recebida (atual)
			float rebaixamento = getRebaixamento(nivelAtual, medInicial.getNivel());
			logger.log("Rebaixamento: " + rebaixamento);
			
			if(rebaixamento == 0)
				cesp = 0;
			else
				cesp = vazao / rebaixamento;
			logger.log("CESP Calculada: "+cesp+" para o poço: " + this.code);
		}
		return cesp;
	}
		
	/**
	 * Calcula diferença de tempo entre o tempo atual e o início do bombeamento
	 * @param dataAtual
	 * @param dataInicial
	 * @return
	 */
	private float diffTempo(java.util.Date dataAtual, java.util.Date dataInicial){
		float diff = 0;

		//Diferença de tempo entre a medida atual e o início do bombeamento.
		Calendar calAtual = Calendar.getInstance();
		calAtual.setTime(dataAtual);
		Calendar calOld = Calendar.getInstance();
		calOld.setTime(dataInicial);
		logger.log("Tempo na medida inicial: "+calOld.getTime().toString());
		logger.log("Tempo na medida atual: "+calAtual.getTime().toString());
		diff = calAtual.getTimeInMillis() - calOld.getTimeInMillis();
		diff = diff/1000; // Tempo em segundos
		logger.log("Diferença de tempo em segundos: " + diff);
		
		return diff;
	}
	
	/** 
	 * Recupera rebaixamento a partir da base de dados
	 * @return Rebaixamento considerando tempo de inicio de bombeamento e a hora atua
	 */
	private float getRebaixamento(float nivelAtual, float nivelInicial) throws Exception {
		float rebaixamento = 0;
		
		rebaixamento = nivelInicial - nivelAtual;
		logger.log("Rebaixamento no período: "+rebaixamento);
		if(rebaixamento <0) {
			rebaixamento = 0;
		}		
		return rebaixamento;		
	}
	
	/**
	 * Recupera medida inicial do dia, baseado no fato de haver corrente (bombeamento)
	 * @return Objeto do tipo medida (contém horário inicial e nível)
	 */
	private Medida getMedidaInicial() throws Exception{
		String SQL_GET_NIVEL_INICIAL = "" +
				" select FIRST 1 data, nivel, hidrometro from grandezas" +
				" where code = '"+this.code+"'" +
				" and data > '"+this.dataFormatoFirebird+"'" + 
				" and nivel > 0" +
				" and corrente > 1" +
				" order by data asc";
		
		
		Medida med = new Medida();
		if ( (this.dataFormatoFirebird==null) || (this.dataFormatoFirebird.equals(""))){
			logger.log("Não vou fazer SQL de medida inicial");
			med.setDate(Calendar.getInstance().getTime());
		} else {
			logger.log(SQL_GET_NIVEL_INICIAL.trim());
			ResultSet rs = db.execQuery(SQL_GET_NIVEL_INICIAL);
			if(rs.next()) {
				med = new Medida();
				med.setNivel(rs.getFloat("nivel"));
				med.setDate(rs.getDate("data"));
				med.setHidrometro(rs.getFloat("hidrometro"));
			}
		}
		return med;		
	}

	public static void main(String[] args) {
    	Database db = null;
    	try {
            db = new Database("SYSDBA","masterkey");	                	
        }catch (Exception e) {
        	System.out.println("Problema ao conectar na base "+e.getMessage());	                	
        }    	
    	Date data = Calendar.getInstance().getTime();
    	Date today = new Date(System.currentTimeMillis());
    	Logger logger = new Logger("451707E", today);
		Cesp cesp = new Cesp(db, logger, "451707E","2015-09-28 10:08:00");
		Date dataAgora = Calendar.getInstance().getTime();
		
		try {
			float cespCalculada = cesp.calculaCesp(17000f, 15f);
			System.out.println("CESP Calculada: " + cespCalculada);			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}