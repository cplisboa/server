import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.Socket;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;
import java.util.StringTokenizer;

/**
 * Thread responsável por Parsear dados recebidos no socket
 * @author Cleo Lisboa
 */
public class ParseData extends Thread {
	private String dadosArquivo = "";
	private String abcissa = "";
	private Socket socket;
	private BufferedReader read;
	private BufferedWriter out;
	private String lastNivel = "0";
	private Logger logger;
	private Date today;
	private String fileToRead = null;
	private boolean debugMode = false;
		
	/** Construtor que recebe BufferedReader para tratamento em thread em separado */
	public ParseData (Socket serverSocket){
		socket = serverSocket;
		try {
			read = new BufferedReader(new InputStreamReader(socket.getInputStream()));
			out = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
		} catch (IOException e) {
			System.out.println("Erro abrindo socket para leitura.");
			e.printStackTrace();
		}
	}
	
	/** Construtor que recebe arquivo para ler para DEBUG*/
	public ParseData (String fileToRead){
		this.fileToRead = fileToRead;
		this.debugMode = true;
		FileReader fr;
		try {
			fr = new FileReader(fileToRead);
			read = new BufferedReader(fr);
		} catch (FileNotFoundException e) {
			System.out.println("Erro abrindo arquivo em modo de debug: "+fileToRead);
			e.printStackTrace();
		}		
	}
	
    public void run() {   
        DateFormat dateFormatter = DateFormat.getDateInstance(DateFormat.DEFAULT, new Locale("pt", "BR"));	                    
        today = new Date(System.currentTimeMillis());        
        SimpleDateFormat formataHora = new SimpleDateFormat("hh:mm");
        System.out.println("Horário da conexão e recebimento dos dados.. "+dateFormatter.format(today)+" - "+formataHora.format(today));
        
        if(!debugMode){
	        try {	                    	
	            boolean hasData = false;		            	
	            for(int j=0; j<20; j++) {
	            	System.out.println("Verificando dados no socket");
		            if (read.ready()) {
		            	System.out.println("Liberado");
		            	hasData = true;
		            	break;
		            } else {
		            	try{
		            		System.out.println("Tentativa: #"+j+" - Vou aguardar mais 10 segs (20 tentativas)");
		            		Thread.sleep(10000);
		            	} catch (Exception e) {
		            		System.out.println("Excessão aguardando dados no socket");
		            	}
		            }
	            }            	
	            if (hasData) {
	            	receiveData();
	            } else {
	            	System.out.println("---- Desistindo dessa conexão, não vieram dados. ------");
	            }
	        }catch (Exception e) {
	        	System.out.println("Ocorreu um erro enquanto aguardávamos os dados: " + e.getMessage());
	        	e.printStackTrace();
	        }
        } else {
        	receiveData();
        }	        
    }//Fim do método RUN
    
    private String readSocket() {
    	String linha="";
    	try {
    		linha = read.readLine();
    	} catch (Exception e) {
    		e.printStackTrace();
    	}
    	dadosArquivo+=linha+"\n";    	
    	return linha;
    }
    
    /**
     * Método responsável por receber os dados do socket e tratar.
     */
    private void receiveData() {
    	Database db = null;
    	try {
            db = new Database("SYSDBA","masterkey");	                	
        }catch (Exception e) {
        	System.out.println("Problema ao conectar na base "+e.getMessage());	                	
        }    	
        
        try {
        	
        	/** Recebe 2 bytes: 	
        		1o - Versão do FW
        		2o - Numero de Sério */ 
        	System.out.println("Lendo versão de FW e serial");
        	String linha = readSocket();
        	StringTokenizer strToken = new StringTokenizer(linha, ",");
        	
        	String fw = strToken.nextToken().trim();
        	String serial = strToken.nextToken().trim();
        	
        	// Aqui o Data-logger aguarda 10 segundos o envio de algum comando
        	out.write("1");
        	
        	linha = readSocket();
        	System.out.println("O que foi enviado após enviarmos o '1': "+linha);
        	
        	
        	
		    System.out.println("Lendo coordenadas UTM");		            				            	
			linha = readSocket();
			System.out.println(linha);
			
			strToken = new StringTokenizer(linha, ",");
			String setor = strToken.nextToken().trim();
			abcissa = strToken.nextToken().trim();
			logger = new Logger(abcissa, today);
			String ordenada = strToken.nextToken().trim();

			//Lendo intervalo entre medidas
			logger.log("Lendo intervalo entre medidas e número de medidas em cada pacote ");
			linha = readSocket();
			logger.log("> "+linha);
			StringTokenizer strData = new StringTokenizer(linha, ",");
			int tempoEntreMedidas = Integer.parseInt(strData.nextToken().trim()); 
			int numMedidas = Integer.parseInt(strData.nextToken().trim());
			
		    do {
	    		linha=readSocket();		    		
	    		if (linha.trim().equals("fim")){
	    			logger.log("Recebido fim do envio de dados. FIM.");
	    			break;
	    		}		    		
		    			    	
	    		logger.log("Começando a ler dados que identificam os campos que virão...");
	    		logger.log("> "+linha);
			    //Parseando Grandezas
				ArrayList<Integer> grandezasList = numGrandezas(linha);
								
			    //Lendo dia e hora
				logger.log("Lendo data e depois hora..");
			    String date = readSocket(); //4a linha
			    String hour = readSocket(); //5a linha
			    logger.log("> "+date);
			    logger.log("> "+hour);
			    String horaFormatada = "";
			    Date horaDate = null;
			    String novaData = "";
			
				try {
					logger.log("Parseando data...");
					strData = new StringTokenizer(date, "/");
						
					String dia = strData.nextToken().trim(); //Pega o dia
				    if (dia.length() == 1)
				    	dia = "0"+dia;
			
					String mes = strData.nextToken().trim(); //Pega o mes
					if (mes.length() == 1)
						mes = "0"+mes;
					String ano = "20" + strData.nextToken().trim(); //Pega o ano		        			        				        			
					novaData = ano + "-" + mes + "-" + dia;   // formato novo '2013-06-15 22:15:00'		        			
					logger.log("Data formatada: "+novaData);	        			
						
				} catch (Exception e) {  
					logger.log("Erro parseando data "+e);  
				}		        		
			
				try {
					logger.log("Parseando hora...");
					SimpleDateFormat formataHora = new SimpleDateFormat("HH:mm");			
			        horaDate = formataHora.parse(hour);
			        horaFormatada = formataHora.format(horaDate);		                
			        logger.log("Hora formatada: "+horaFormatada);
						
				} catch (Exception e) {  
					logger.log("Erro parseando hora "+e);  
				}  	        		
				            	  				
				//Lendo os dados mais frequentes (pacotes de dados com numMedidas)
				String lastHour = ""; //Ultima hora utilizada
				int periodoGrandezas = (numMedidas+1) * tempoEntreMedidas; // Retorna período em segundos do envio de grandezas
				for(int i=0; i<numMedidas; i++) {
					linha=readSocket();	
					lastHour = lineData(i, linha, tempoEntreMedidas, grandezasList, db, horaDate, novaData);
				}
				logger.log("Terminei de ler os dados de nível. Foram: "+numMedidas+ "linhas.");
		    	
				logger.log("Identificando IDs das grandezas..");
	    		linha=readSocket();
	    		logger.log("> "+linha);
	    		grandezasList = numGrandezas(linha);
	    		
	    		logger.log("Os dados são esses: "+linha);
	    		linha=readSocket();
	    		Grandeza grandeza = new Grandeza(abcissa, logger);
	    		grandeza.parseGrandeza(grandezasList, periodoGrandezas, linha, lastHour, lastNivel);
	    		
		    } while(true);
		    logger.log("Fim do tratamento dos dados. Fechando socket, encerrando a Thread");
		    logger.log("¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨");
		    read.close();
		    socket.close();
        } catch (IOException ex) {
        	logger.log("Erro lendo dados na parsedata " + ex);
        } finally {
        	db.close();
        	logger.close();
         }
	    //Salvando em arquivo
	    saveFile(dadosArquivo);
	    logger.close();
    }
    
    /** Faz o parsing dos valores lidos na linha. Retorna horário da ultima hora utilizada */
	private String lineData(int counter, String linha, int tempoMedidas, ArrayList<Integer> grandezas, Database db, Date horaDate, String novaData){
		String lastHour="";
		ArrayList<String> valueList = new ArrayList<String>();
		
		logger.log(linha);
	   	//Parseando
		StringTokenizer str = new StringTokenizer(linha, ",");
		//Primeira dado é sempre o TEMPO
		//TODO isso precisa ser alterado para salvar o numero de grandeza e seu valor.
	  
	   	String nivel = str.nextToken();
	   	String vazao = "0"; //Fixo em zero até alterarmos a tabela do banco de dados
	   	String corrente = "0"; //Fixo em zero
	       	
	   	//Iterando pelos dados recebidos, a 4a grandeza (iterator=3) é depois de ter lido tempo, nivel e vazao
	   	int iteratorGrandezas = 3;
	   	if (str.hasMoreTokens()) {
	       	do {			            		
	           	String value = str.nextToken();

		   		int gran = grandezas.get(iteratorGrandezas);
		   		if(gran==2) { //2 é o código de corrente
		   		    corrente = value;
		   		    logger.log("corrente = "+corrente);
		   		} else {
		       		valueList.add(value);
		       		logger.log("Valor adicionado: "+value);
		   		}
		   		iteratorGrandezas++;
	    	} while (str.hasMoreTokens());
	   	}	   	
	   	logger.log("--------------------------------------------");
	
		Calendar cal = Calendar.getInstance();            		
		cal.set(2013, 1, 1, horaDate.getHours(), horaDate.getMinutes());
		//Adicionando tempo entre os steps
		logger.log("Somando "+(counter * tempoMedidas)+ " segundos ao relógio.");
		cal.add(Calendar.SECOND, (counter * tempoMedidas));
	
		//inserindo dados
		try {
			logger.log("Inserindo na base de dados..");
			lastHour = insertData(db, 1,novaData,""+cal.get(Calendar.HOUR_OF_DAY),""+cal.get(Calendar.MINUTE),nivel,vazao,corrente, abcissa);		            		
		} catch (Exception e) {
			logger.log("Erro inserindo no db "+e.getMessage());
			e.printStackTrace();
		}		        			            	
		lastNivel = nivel;
		return lastHour;
	}
    
private ArrayList<Integer> numGrandezas (String linha){
	logger.log(linha);
    StringTokenizer str = new StringTokenizer(linha, ",");

    ArrayList<Integer> list = new ArrayList<Integer>();
    do {			            		
       	String grandeza = str.nextToken();
    	list.add(new Integer(grandeza));
    	logger.log("Grandeza: "+grandeza);
    } while (str.hasMoreTokens());
    logger.log("Fim das grandezas.. Temos " + list.size() + " Grandezas nesse arquivo" );
    
	return list;
}
    
// Inserção na tabela SIGAS_POCOS  
// Retorna ultimo horário utilizado
public String insertData(Database db, int id, String data, String hora, String minuto, String nivel, String vazao, String corrente, String abcissa) {
	// insert into sigas_pocos values (1, '2013-06-09 22:29:05', 16.21, 10, 0, 110000, 'juper');
	String horaFormatada = "";
	String tsInsert = "";
	
    if (hora.length() == 1)
    	hora = "0" + hora;
    
    if (minuto.length() == 1)
    	minuto = "0" + minuto;
    
    horaFormatada = hora+":"+minuto+":00";
    tsInsert = data+" "+horaFormatada;

	if(db==null)
		logger.log("Conexão com o banco é nula aqui!!!!...");
	String sql = "INSERT INTO sigas_pocos VALUES ("+ id + ", '"+ tsInsert +"', "+nivel+", "+vazao+", "+corrente+", 0,'"+abcissa+"')";
	logger.log(sql);
	db.insert(sql);
	return tsInsert; //retornando ultima hora para ser utilizada
}	    
    
	    
	//***********************Rotina que  Salva em arquivo *************************/
	private void saveFile(String dados){
		try {
			Calendar date = Calendar.getInstance();
			
			  String fileName = null;
			  fileName = abcissa +"-"+ date.get(Calendar.DAY_OF_MONTH)+"-"+(date.get(Calendar.MONTH)+1)+"-"+date.get(Calendar.YEAR)+"-";
			  fileName += date.get(Calendar.HOUR_OF_DAY)+"."+date.get(Calendar.MINUTE)+"."+date.get(Calendar.SECOND);
			  fileName += "gprs data.txt";			
			FileWriter fis = new FileWriter(fileName);
			BufferedWriter escritor = new BufferedWriter(fis);
			escritor.write(dados);
			escritor.close();
			
			fis.close();
		}catch(Exception e){
			e.printStackTrace();
		}	
	}	    
	    
} // Fim da classe ParseData