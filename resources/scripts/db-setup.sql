CREATE SCHEMA `pedido`;

CREATE TABLE `cliente` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `cpf` VARCHAR(45) NOT NULL,
  `nome` VARCHAR(45) NULL,
  `sobrenome` VARCHAR(45) NULL,
  PRIMARY KEY (`id`),
  UNIQUE INDEX `id_UNIQUE` (`id` ASC) VISIBLE,
  UNIQUE INDEX `cpf_UNIQUE` (`cpf` ASC) VISIBLE);

CREATE TABLE `pedido` (
  `id` int NOT NULL AUTO_INCREMENT,
  `data` date NOT NULL,
  `cliente_fk` int NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `id_UNIQUE` (`id`),
  KEY `cliente_fk_idx` (`cliente_fk`),
  CONSTRAINT `cliente_fk` FOREIGN KEY (`cliente_fk`) REFERENCES `cliente` (`id`) ON DELETE CASCADE ON UPDATE CASCADE)

CREATE TABLE `produto` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `descricao` VARCHAR(120) NULL,
  PRIMARY KEY (`id`),
  UNIQUE INDEX `id_UNIQUE` (`id` ASC) VISIBLE);

CREATE TABLE `produto_pedido` (
   `produto_fk` int NOT NULL,
   `pedido_fk` int NOT NULL,
   `quantidade` int NOT NULL,
   KEY `pedido_fk_idx` (`pedido_fk`),
   KEY `produto_fk_idx` (`produto_fk`),
   CONSTRAINT `pedido_fk` FOREIGN KEY (`pedido_fk`) REFERENCES `pedido` (`id`),
   CONSTRAINT `produto_fk` FOREIGN KEY (`produto_fk`) REFERENCES `produto` (`id`))