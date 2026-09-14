-- Um banco por serviço: cada um tem seu próprio histórico Flyway e nenhuma tabela é compartilhada.
CREATE DATABASE orders;
CREATE DATABASE payments;
CREATE DATABASE inventory;
CREATE DATABASE notifications;
CREATE DATABASE audit;
