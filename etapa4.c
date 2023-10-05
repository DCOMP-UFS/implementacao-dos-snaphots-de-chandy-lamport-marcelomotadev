/*
 *      Etapa 4 - Projeto PPC
 *      Alunos: Marcelo Mota e João Fraga
 *      Compilação: mpicc -pthread etapa4.c -o etapa4
 *      Execução:   mpiexec -n 3 ./etapa4
*/


#include <stdio.h>
#include <string.h>  
#include <mpi.h>   
#include <stdlib.h>
#include <pthread.h> 
#include <unistd.h>
#include <semaphore.h>
#include <time.h>


#define BUFFER_SIZE 4   // Númermo máximo de relógios enfileiradas

int realizandoSnapshot = 0;
int controleMarcadorRecebido[3];



// Estrutura que representa o Relógio
typedef struct Clock{
    int p[4];
    // p[0-2]: Relógio
    // p[3]  : Origem ou Destino do Relógio
} Clock;

// Estrutura que representa um Relógio que vai estar no Snap
typedef struct Snap{
    int p[4];
    struct Snap* next;
} Snap;

// Estrutura que representa o Snap de um processo
typedef struct SnapShot{
    Snap snap_relogio;
    Snap* snap_filas;
} SnapShot;



Clock process_clock_global;



// Filas de Envio e de Recebimento de Relógios de cada Processo
Clock FilaRelogiosEnviado[BUFFER_SIZE];
Clock FilaRelogiosRecebidos[BUFFER_SIZE];

// Contadores das Filas de Envio e de Recebimento de cada Processo
int countRelogiosEnviados;
int countRelogiosRecebidos;

// Variaveis Mutex das Filas de Envio e de Recebimento de cada Processo
pthread_mutex_t mutexFilaRelogiosEnviados;
pthread_mutex_t mutexFilaRelogiosRecebidos;
pthread_mutex_t mutexRelogio;


//Variáveis de Condição Full e Empty das Filas de Envio e de Recebimento de cada Processo
pthread_cond_t condFullFilaRelogiosEnviados;
pthread_cond_t condEmptyFilaRelogiosEnviados;
pthread_cond_t condFullFilaRelogiosRecebidos;
pthread_cond_t condEmptyFilaRelogiosRecebidos;






/* ------------ DECLARAÇÃO DAS FUNÇÕES ------------ */
// Função que remove um relógio da Fila de Envio
Clock get_RelogioFilaEnviados();

// Função que remove um relógio da Fila de Recebimento
Clock get_RelogioFilaRecebidos();


// Função que insere um relógio na Fila de Envio
void submit_RelogioFilaEnviados(Clock clock_enviado);

// Função que insere um relógio na Fila de Recebidos
void submit_RelogioFilaRecebidos(Clock clock_recebido);


// Incrementa a posição do Relógio do Processo
void IncrementaRelogioProcesso(Clock* process_clock, int process_rank);

// Compara os valores das posições do Relógio do Processo com os valores das posições do Relógio Recebido
void ComparaRelogioProcessoRelogioRecebido(Clock* process_clock, int process_rank, Clock recv_clock);


// Função Evento Interno do Processo
void EventoInternoProcesso(Clock* process_clock, int process_rank);

// Função Send do Processo
void EventoSendProcesso(Clock* process_clock, int process_rank, int destino);

// Função Receive do Processo
void EventoReceiveProcesso(Clock* process_clock, int process_rank);

// Função que Inicia o Snapshot no Processo Princiapl
void EventoSnapshotProcesso(Clock* process_clock, int process_rank);


// Função que imprime o evento que foi realizado
void ImprimeEvento(Clock* process_clock, int process_rank, int source_dest_rank, int evento);


// Função que 'starta' a Thread de Envio
void *startThreadEnvio(void* args);

// Função que 'starta' a Thread de Recebimento
void *startThreadRecebimento(void* args);


// Função que o Processo 0 executa
void process0(int my_process);

// Função que o Processo 1 executa
void process1(int my_process);

// Função que o Processo 2 executa
void process2(int my_process);









/* -------------- FUNÇÃO PRINCIPAL -------------- */
int main(int argc, char* argv[]){
    // Para gerar randômicos diferentes a cada execução do código
    srand(time(NULL));
    
    // Inicialização do MPI
    MPI_Init(NULL,NULL);
    
    // Declaração da variável que identifica o processo corrente
    int process_rank;
    MPI_Comm_rank(MPI_COMM_WORLD,&process_rank);
    
    // Variável para informa as Threads em qual Processo elas foram criadas | Tipo (long)
    long id_process = (long) process_rank;
    
    // Inicialização do Mutex de Envio
    pthread_mutex_init(&mutexFilaRelogiosEnviados,NULL);
    
    // Inicialização do Mutex de Recebimento
    pthread_mutex_init(&mutexFilaRelogiosRecebidos,NULL);
    
    // Inicialização do Mutex do Relógio
    pthread_mutex_init(&mutexRelogio,NULL);
    
    
    // Inicialização das Variáveis de Condição de Envio
    pthread_cond_init(&condFullFilaRelogiosEnviados,NULL);
    pthread_cond_init(&condEmptyFilaRelogiosEnviados,NULL);
    
    // Inicialização das Variáveis de Condição de Recebimento
    pthread_cond_init(&condFullFilaRelogiosRecebidos,NULL);
    pthread_cond_init(&condEmptyFilaRelogiosRecebidos,NULL);
    
    

    
    

    
    
    // Execução dos respectivos processos
    if(process_rank == 0){
        process0(process_rank);
    } else if(process_rank == 1){
        process1(process_rank);
    } else if(process_rank == 2){
        process2(process_rank);
    } else{
        printf("\n*** PROCESSO %d NAO DEFINIDO\n\n", process_rank);
    }
    
    

    
    
    // Encerramento do Mutex de Envio
    pthread_mutex_destroy(&mutexFilaRelogiosEnviados);
    
    // Encerramento do Mutex de Recebimento
    pthread_mutex_destroy(&mutexFilaRelogiosRecebidos);
    
    
    // Encerramento das Variáveis de Condição de Envio
    pthread_cond_destroy(&condFullFilaRelogiosEnviados);
    pthread_cond_destroy(&condEmptyFilaRelogiosEnviados);
    
    // Encerramento das Variáveis de Condição de Envio
    pthread_cond_destroy(&condFullFilaRelogiosRecebidos);
    pthread_cond_destroy(&condEmptyFilaRelogiosRecebidos);    

    
    // Finalização do MPI
    MPI_Finalize();
    
    return 0;
}









/* ------------ IMPLEMENTAÇÃO DAS FUNÇÕES ------------ */

// Função que remove um relógio da Fila de Relógios Enviados
Clock get_RelogioFilaEnviados(){
    // Bloqueia o mutex
    pthread_mutex_lock(&mutexFilaRelogiosEnviados);
    
    // Equanto não tiver relógios na Fila de Envio
    while(countRelogiosEnviados == 0){
        // Fica esperando um relógio ser inserido na Fila de Envio
        pthread_cond_wait(&condEmptyFilaRelogiosEnviados,&mutexFilaRelogiosEnviados);
    }
    
    // Pega o primeiro relógio da Fila de Envio
    Clock first_clock_envio = FilaRelogiosEnviado[0];
    
    // Decrementa a quantidade de relógios da Fila de Envio
    countRelogiosEnviados--;
    
    // Reorganiza a Fila de Envio
    for(int i = 0; i < countRelogiosEnviados; i++){
        FilaRelogiosEnviado[i] = FilaRelogiosEnviado[i+1];
    }
    
    // Desbloqueia o mutex
    pthread_mutex_unlock(&mutexFilaRelogiosEnviados);
    
    // Manda um sinal avisando que foi retirado um relógio da Fila de Envio
    pthread_cond_signal(&condFullFilaRelogiosEnviados);
    
    // Retorna o relógio removido
    return first_clock_envio;
}

// Função que remove um relógio da Fila de Recebidos
Clock get_RelogioFilaRecebidos(){
    // Bloqueia o mutex
    pthread_mutex_lock(&mutexFilaRelogiosRecebidos);
    
    // Enquanto não tiver relógios na Fila de Recebidos
    while(countRelogiosRecebidos == 0){
        // Fica esperando um relógio a ser inserido na Fila de Recebidos
        pthread_cond_wait(&condEmptyFilaRelogiosRecebidos,&mutexFilaRelogiosRecebidos);
    }
    
    // Pega o primeiro relógio da Fila de Recebidos
    Clock first_clock_recebido = FilaRelogiosRecebidos[0];
    
    // Decrementa a quantidade de relógios da Fila de Recebidos
    countRelogiosRecebidos--;
    // Reorganiza a Fila de Recebidos
    
    for(int i = 0; i < countRelogiosRecebidos; i++){
        FilaRelogiosRecebidos[i] = FilaRelogiosRecebidos[i+1];
    }
    
    // Desbloqueia o mutex
    pthread_mutex_unlock(&mutexFilaRelogiosRecebidos);
    
    // Manda um sinal avisando que foi retirado um relógio da Fila de Recebidos
    pthread_cond_signal(&condFullFilaRelogiosRecebidos);
    
    // Retorna o relógio removido
    return first_clock_recebido;
}


// Função que insere um relógio na Fila de Envio
void submit_RelogioFilaEnviados(Clock clock_enviado){
    // Bloqueia o mutex
    pthread_mutex_lock(&mutexFilaRelogiosEnviados);
    
    // Enquanto a Fila de Envio estiver cheia
    while(countRelogiosEnviados == BUFFER_SIZE){
        // Fica esperando um relógio ser removido da Fila de Envio
        pthread_cond_wait(&condFullFilaRelogiosEnviados,&mutexFilaRelogiosEnviados);
    }
    
    // Insere o relógio no final da Fila de Envio
    FilaRelogiosEnviado[countRelogiosEnviados] = clock_enviado;
    
    // Incrementa a quantidade de relógios da Fila de Envio
    countRelogiosEnviados++;
    
    // Desbloqueia o mutex
    pthread_mutex_unlock(&mutexFilaRelogiosEnviados);
    
    // Manda um sinal avisando que foi inserido um relógio na Fila de Envio
    pthread_cond_signal(&condEmptyFilaRelogiosEnviados);
}

// Função que insere um relógio na Fila de Recebidos
void submit_RelogioFilaRecebidos(Clock clock_recebido){
    // Bloqueia o mutex
    pthread_mutex_lock(&mutexFilaRelogiosRecebidos);
    
    // Enquanto a Fila de Recebidos estiver cheia
    while(countRelogiosRecebidos == BUFFER_SIZE){
        // Fica esperando um relógio ser removido da Fila de Recebidos
        pthread_cond_wait(&condFullFilaRelogiosRecebidos,&mutexFilaRelogiosRecebidos);
    }
    
    // Insere o relógio no final da Fila de Recebidos
    FilaRelogiosRecebidos[countRelogiosRecebidos] = clock_recebido;
    
    // Incrementa a quantidade de relógios da Fila de Recebidos
    countRelogiosRecebidos++;
    
    // Desbloqueia o mutex
    pthread_mutex_unlock(&mutexFilaRelogiosRecebidos);
    
    // Manda um sinal avisando que foi inserido um relógio na Fila de Recebidos
    pthread_cond_signal(&condEmptyFilaRelogiosRecebidos);
}


// Incrementa a posição do Relógio do Processo
void IncrementaRelogioProcesso(Clock* process_clock, int process_rank){
    // Incrementa o a posição do Relógio do Processo atual
    process_clock->p[process_rank]++;;
}

// Compara os valores das posições do Relógio do Processo com os valores das posições do Relógio Recebido
void ComparaRelogioProcessoRelogioRecebido(Clock* process_clock, int process_rank, Clock recv_clock){
    // Para todas as posições 'i' do Relógio
    for(int i = 0; i < 3; i++){
        // Se a posição do Relógio é diferente do Processo atual
        if(i != process_rank){
            // Se o valor da posição 'i' do Relógio Recebido é maior do que o valor da posição 'i' do Relógio do Processo atual
            if(recv_clock.p[i] > process_clock->p[i]){
                // Atualiza o valor da posição 'i' do Relógio do Processo atual
                process_clock->p[i] = recv_clock.p[i];
            }
        }
    }
}


// Função Evento Interno do Processo
void EventoInternoProcesso(Clock* process_clock, int process_rank){
    // Atualiza Relógio do Processo atual
    IncrementaRelogioProcesso(&process_clock_global,process_rank);
    
    // Imprime o Relógio do Processo após um Evento Interno
    ImprimeEvento(&process_clock_global,process_rank,-1,1);
}

// Função Send do Processo
void EventoSendProcesso(Clock* process_clock, int process_rank, int destino){
    // Atualiza Relógio do Processo atual
    IncrementaRelogioProcesso(&process_clock_global,process_rank);
    
    // Define o destino do Relógio do Processo atual
    process_clock_global.p[3] = destino;

    // Insere o Relógio do Processo atual na Fila de Relógios Enviados
    submit_RelogioFilaEnviados(process_clock_global);
    
    // Imprime o Relógio do Processo atual após um Evento Send
    ImprimeEvento(&process_clock_global,process_rank,process_clock_global.p[3],2);
}

// Função Receive do Processo
void EventoReceiveProcesso(Clock* process_clock, int process_rank){
    // Pega o primeiro relógio da Fila de Relógios Recebidos
    Clock recv_clock = get_RelogioFilaRecebidos();
    
    // Atualia Relógio
    IncrementaRelogioProcesso(&process_clock_global,process_rank);
    ComparaRelogioProcessoRelogioRecebido(&process_clock_global,process_rank,recv_clock);
    
    // Imprime o Relógio do Processo atual após um Evento Receive
    ImprimeEvento(&process_clock_global,process_rank,recv_clock.p[3],3);
}

// Função que Inicia o Snapshot no Processo Inicializador
void EventoSnapshotProcesso(Clock* process_clock, int process_rank){
    // Bloqueia o Relógio Atual do Processo (no caso, o Processo 0)
    pthread_mutex_lock(&mutexRelogio);
    
    // Marca como "Snapshot em andamento"
    realizandoSnapshot = 1;
    
    // Para marcar de quais processos o processo atual já reebeu marcadores
    controleMarcadorRecebido[process_rank] = 1;
    
    
    printf("Processo Inicializou o SnapShot: %d\n", realizandoSnapshot);
    
    // Capta o Relógio Atual Do Processo
    printf("Relogio (%d): [%d %d %d]\n", process_rank, process_clock_global.p[0], process_clock_global.p[1], process_clock_global.p[2]); 
    
    // Desbloqueia o Relógio Atual do Processo
    pthread_mutex_unlock(&mutexRelogio);
    
    // Cria Marcador
    Clock marcador = {{-1,-1,-1,-1}};
    
    
    // Enviao marcador para os demais processos
    // Para todos os processos
    for(int j = 0; j < 3; j++){
        // Exceto o processo atual // 1
        if(j != process_rank){
            // Marca o destino do marcador
            marcador.p[3] = j;
            // Insere o marcador na Fila de Envio
            submit_RelogioFilaEnviados(marcador);
        }
    }
}


// Função que imprime o evento que foi realizado
void ImprimeEvento(Clock* process_clock, int process_rank, int source_dest_rank, int evento){
    switch(evento){
        case 0: // Situação Inicial
            printf("Processo %d | Inicial      | [%d %d %d]\n", process_rank, process_clock->p[0], process_clock->p[1], process_clock->p[2]);
            break;
        case 1: // Evento Interno
            printf("Processo %d | Interno      | [%d %d %d]\n", process_rank, process_clock->p[0], process_clock->p[1], process_clock->p[2]);
            break;
        case 2: // Evento Send
            printf("Processo %d | Envio    (%d) | [%d %d %d]\n", process_rank, source_dest_rank, process_clock->p[0], process_clock->p[1], process_clock->p[2]);
            break;
        case 3: // Evento Receive
            printf("Processo %d | Recebido (%d) | [%d %d %d]\n", process_rank, source_dest_rank, process_clock->p[0], process_clock->p[1], process_clock->p[2]);
            break;
        default:
            printf("Evento Invalido\n");
    }
}


// Função que 'starta' a Thread de Envio
void *startThreadEnvio(void* args){
    // Identificação do Processo que startou essa Thread de Envio
    long id = (long) args;
    
    // Variável que armazena o destino do relógio (p[3])
    int destino;
    
    // Loop infinito
    while(1){
        // Pega o primeiro relógio da Fila de Relógios Enviados
        Clock clock_envio = get_RelogioFilaEnviados();
        
        // Define o destino do Relógio
        destino = clock_envio.p[3];
        
        // Envia o Relógio para o Processo de Destino
        MPI_Send(&clock_envio,sizeof(Clock),MPI_BYTE,destino,0,MPI_COMM_WORLD);
    }
}

// Função que 'starta' a Thread de Recebidos
void *startThreadRecebimento(void* args){
    // Identificação do Processo que startou essa Thread de Recebidos
    long id = (long) args;
    
    // Variável status do Relógio recebido
    MPI_Status status; 
    
    // Variável que armazena a origem do Relógio recebido
    int origem;
    
    // Loop infinito
    while(1){
        // Declara variável que armazena o Relógio recebido
        Clock clock_recebido;
        
        // Recebe o Relógio de qualquer Processo
        MPI_Recv(&clock_recebido,sizeof(Clock),MPI_BYTE,MPI_ANY_SOURCE,0,MPI_COMM_WORLD,&status);
        
        // Se chegou um marcador no processo
        if(clock_recebido.p[0] == -1){
            // Define a origem do marcador
            origem = status.MPI_SOURCE;
            printf("Chegou no processo (%ld) um marcador do Processo (%d)\n", id, origem);
            
            
            // Marca o recebimento de um marcador pelo canal de origem
            controleMarcadorRecebido[origem] = 1;
            printf("Processo (%ld) fechou o canal do Processo (%d)\n", id, origem);
            
            // Se o processo não está realizando um snapshot
            if(realizandoSnapshot == 0){
                // Espera a Fila de Recebimento Ficar Vazia
                while(countRelogiosRecebidos != 0){
                    printf("Esperando\n");
                }
                // Faz o SNAP do relógio atual do Processo
                pthread_mutex_lock(&mutexRelogio);
                printf("Relogio (%ld): [%d %d %d]\n", id, process_clock_global.p[0], process_clock_global.p[1], process_clock_global.p[2]); 
                pthread_mutex_unlock(&mutexRelogio);
                
                // Marca como realizando SnapShot
                realizandoSnapshot = 1;
                // Para funcionamento do programa
                controleMarcadorRecebido[id] = 1;
                
                printf("Processo (%ld) nao estava, mas comecou um snapshot: %d\n", id, realizandoSnapshot);
                
                // Cria Marcador
                Clock marcador = {{-1,-1,-1,-1}};
                
                
                // Adiciona na Fila de Envio os marcadores para os demais processos
                for(int j = 0; j < 3; j++){
                    if(j != id){
                        marcador.p[3] = j;
                        submit_RelogioFilaEnviados(marcador);
                    }
                }
                
            }
            // Se o processo está realizando um snapshot
            else{
                // Se já recebeu marcador de todos os canais
                if(controleMarcadorRecebido[0] == 1 && controleMarcadorRecebido[1] == 1 && controleMarcadorRecebido[2] == 1){
                    // Marca como SnapShot finalizado
                    realizandoSnapshot = 0;
                    // Reseta o controle dos canais de entrada
                    for(int i = 0; i < 3; i++){
                        controleMarcadorRecebido[i] = 0;
                    }
                    printf("Processo (%ld) estava em snapshot, mas agora SIM ENCERROU: %d\n", id, realizandoSnapshot);
                }else{
                    printf("Processo (%ld) esta em snapshot, ainda NAO ENCERROU: %d\n", id, realizandoSnapshot);
                }
                
            }

            // Pule para a próxima iteração do while
            continue;
        }
        
        // Se está em processo de SnapShot
        if(realizandoSnapshot == 1){
            // Imprime os relógios recebidos de cada canal durante o Processo do SnapShot
            printf("SnapShot (%ld) | Recebeu o relogio: [%d %d %d] do Processo (%d)\n", id, clock_recebido.p[0], clock_recebido.p[1], clock_recebido.p[2], origem);
        }
        
        // Marca a origem do Relógio (oriundo de um evento)
        clock_recebido.p[3] = origem;
        
        // Insere o Relógio rebido na Fila de Relógio Recebidos
        submit_RelogioFilaRecebidos(clock_recebido);
    }
}


// Função que o Processo 0 executa
void process0(int my_process){
    long processo = (long) my_process;
    // Variável Thread de Envio
    pthread_t threadEnvio;
    
    // Variável Thread de Recebimento
    pthread_t threadRecebimento;
    
    // Criação da Thread de Envio
    if(pthread_create(&threadEnvio,NULL,&startThreadEnvio, (void*) processo) != 0){
        perror("Faile to create the thread\n");
    }
    // Criação da Thread de Recebimento
    if(pthread_create(&threadRecebimento,NULL,*startThreadRecebimento,(void*) processo) != 0){
        perror("Faile to create the thread\n");
    }
    
    
    
    // Relógio Inicial do Processo 0
    // Clock process_clock = {{0,0,0,0}};
    // Inicialização do Relógio Global
    for(int i = 0; i < 4; i++){
        process_clock_global.p[i] = 0;

    }
    
    
    // Imprime o Relógio do processo na situação inicial
    ImprimeEvento(&process_clock_global,0,-1,0);
    
    // Sequência de Eventos do Processo 0
    EventoInternoProcesso(&process_clock_global,0);
    EventoSendProcesso(&process_clock_global,0,1);
    EventoReceiveProcesso(&process_clock_global,0);
    
    // Evento de Inicialização Global do Snapshot 
    EventoSnapshotProcesso(&process_clock_global,0);
    
    EventoSendProcesso(&process_clock_global,0,2);
    EventoReceiveProcesso(&process_clock_global,0);
    EventoSendProcesso(&process_clock_global,0,1);
    EventoInternoProcesso(&process_clock_global,0);
    
    
    // Join na Thread de Envio
    if(pthread_join(threadEnvio,NULL) != 0){
        perror("Faile to join the thread\n");
    }
    // Join na Thread de Recebidos
    if(pthread_join(threadRecebimento,NULL) != 0){
        perror("Faile to join the thread\n");
    }
    
    
}

// Função que o Processo 1 executa
void process1(int my_process){
    long processo = (long) my_process;
    // Variável Thread de Envio
    pthread_t threadEnvio;
    
    // Variável Thread de Recebimento
    pthread_t threadRecebimento;
    
    // Criação da Thread de Envio
    if(pthread_create(&threadEnvio,NULL,&startThreadEnvio, (void*) processo) != 0){
        perror("Faile to create the thread\n");
    }
    // Criação da Thread de Recebimento
    if(pthread_create(&threadRecebimento,NULL,*startThreadRecebimento,(void*) processo) != 0){
        perror("Faile to create the thread\n");
    }
    
    
    
    // Relógio Inicial do Processo 1
    Clock process_clock = {{0,0,0,0}};
    
    // Imprime o Relógio do processo na situação inicial
    ImprimeEvento(&process_clock,1,-1,0);
    
    // Sequência de Eventos do Processo 1
    EventoSendProcesso(&process_clock,1,0);
    EventoReceiveProcesso(&process_clock,1);
    EventoReceiveProcesso(&process_clock,1);
    
    
    // Join na Thread de Envio
    if(pthread_join(threadEnvio,NULL) != 0){
        perror("Faile to join the thread\n");
    }
    // Join na Thread de Recebidos
    if(pthread_join(threadRecebimento,NULL) != 0){
        perror("Faile to join the thread\n");
    }
    
    
}

// Função que o Porcesso 2 executa
void process2(int my_process){
    long processo = (long) my_process;
    // Variável Thread de Envio
    pthread_t threadEnvio;
    
    // Variável Thread de Recebimento
    pthread_t threadRecebimento;
    
    // Criação da Thread de Envio
    if(pthread_create(&threadEnvio,NULL,&startThreadEnvio, (void*) processo) != 0){
        perror("Faile to create the thread\n");
    }
    // Criação da Thread de Recebimento
    if(pthread_create(&threadRecebimento,NULL,*startThreadRecebimento,(void*) processo) != 0){
        perror("Faile to create the thread\n");
    }
    
    
    // Relógio Inicial do Processo 2
    Clock process_clock = {{0,0,0,0}};
    
    // Imprime o Relógio do processo na situação inicial
    ImprimeEvento(&process_clock,2,-1,0);
    
    // Sequência de Eventos do Processo 2
    EventoInternoProcesso(&process_clock,2);
    EventoSendProcesso(&process_clock,2,0);
    EventoReceiveProcesso(&process_clock,2);
    
    
    
    // Join na Thread de Envio
    if(pthread_join(threadEnvio,NULL) != 0){
        perror("Faile to join the thread\n");
    }
    // Join na Thread de Recebidos
    if(pthread_join(threadRecebimento,NULL) != 0){
        perror("Faile to join the thread\n");
    }


}