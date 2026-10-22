# Comentários do orientador no texto

Comentários deixados no Overleaf com `\todoe{...}` e como cada um foi tratado. A macro `\todoe` continua no preâmbulo do `texto/tcc2.tex` para novos comentários.

## Rodada 1 (feitos sobre a versão de 13/10/2026, commit `0dc019a`; tratados em 22/10/2026)

| # | Onde | Comentário | Como foi tratado |
| --- | --- | --- | --- |
| 1 | Introdução, depois do parágrafo da composição broker + mecanismo interno | "Inserir texto aqui para fundamentar o porquê é importante estudar esses cenários" | Parágrafo novo com três razões: comparações existentes avaliam os brokers isolados e em outras plataformas; Channels e Pipelines são recomendados pela Microsoft mas acrescentam fila e troca de thread a cada evento, sem medição de efeito; comparação justa exige ambiente e carga controlados (Fu et al.) |
| 2 | Introdução, logo depois | "Depois entra dizendo... nesse sentido, este trabalho tem por objetivo..." | O objetivo geral passou a começar com "Nesse sentido, o objetivo geral deste trabalho é...", logo depois da motivação. O parágrafo da OpenF1 foi para depois dos objetivos específicos |
| 3 | Fundamentação, "Fontes de Dados: OpenF1" | "Precisa aumentar este texto?" | Ampliada: como os dados são servidos (REST, JSON, por sessão, desde 2023), os conjuntos disponíveis, os limites do acesso gratuito e por que a OpenF1 serve de base de carga (dados reais, muitas fontes ordenadas como uma frota, públicos) |
| 4 | Arquitetura e Implementação, início | "Essa seção está um pouco confusa. Aqui, acredito ser melhor adicionar a figura depois da introdução. Depois usar a figura para guiar o texto da explicação do Cenário Experimental." | Seção reorganizada: introdução curta, a Figura 1 logo em seguida, e uma subseção por etapa da figura (Etapa 1: Dados até Etapa 5: Persistência e Métricas). "Combinações Avaliadas" entrou na Etapa 4, onde a figura mostra os três modos |
| 5 | Arquitetura e Implementação, depois da figura | "Lendo a imagem e seguindo no texto abaixo, me parece que não há um mapeamento direto entre a explicação e os elementos da figura." | Cada subseção usa os nomes dos passos da figura, na mesma ordem e em negrito ou itálico: API OpenF1 e Cópia local; Recorte, Interpolação, Multiplicação e Publicação; Kafka ou RabbitMQ; Consumidor, um modo por rodada e Janelas e detecção; PostgreSQL e Métricas |
| 6 | Processamento | "A lógica de processamento é a mesma biblioteca nos três modos" — "não entendi" | Reescrito: os três modos chamam exatamente o mesmo código de processamento, uma biblioteca compartilhada; o que muda é só o caminho do evento entre o consumidor e esse código, então qualquer diferença entre os modos vem da forma de entrega, e não do cálculo |
