
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Calendar;
/**
 * Classe responsável por iniciar as threads de tratamento para os dados enviados pelos Data Loggers.
 * @author Cleo Lisboa
 */
public class Server {
	private ServerSocket server;
	private Socket socket;
	private static final int PORTA=2020;      
	
	public Server(String readFromFile) {
        try {	       
          	System.out.println("SIGAS  GPRS  Server. Versao  FINAL 1.1  de  05/11/2015.,");
          	System.out.println("Calculo de CESP considerando periodo de bombeamento.");
          	System.out.println("Logs de cada poco em arquivos separados.");
          	System.out.println("Seja bem vindo...");
          	System.out.println("Hora de início: "+Calendar.getInstance().getTime().toString());
          	
		    server = new ServerSocket(PORTA);		    
		    if(readFromFile!=null) {
		    	System.out.println("MODE DE DEBUG! LENDO DE ARQUIVO: "+readFromFile);
            	ParseData thread = new ParseData(readFromFile);
            	thread.start();      				    			    	
		    } else {
		    	System.out.println("Aguardando conexões na porta:"+PORTA);
			    while (true) {			    		            	
	          		socket = server.accept();            		         		
	            	// Iniciar thread de tratamento
	          		System.out.println("Conexão bem sucedida: " + Calendar.getInstance().getTime().toString());
	            	System.out.println("---------------------------------------------");
	            	ParseData thread = new ParseData(socket);
	            	thread.start();      		
	            }		    	
		    }	           	            
        } catch (Exception ex) {
        	System.out.println("Exception não esperada: " + ex.getMessage());
            ex.printStackTrace();
        }        
    }	    
	    
    public static void main(String[] args) {
    	if(args.length > 0)
    		new Server(args[0]);
    	else
    		new Server(null);
    }    
}