CREATE TABLE `users` (
  `id` varchar(20) NOT NULL,
  `name` varchar(50) DEFAULT NULL,
  `last_name` varchar(100) DEFAULT NULL,
  `transport` varchar(10) DEFAULT NULL,
  `age` varchar(100) DEFAULT NULL,
  `gender` varchar(5) DEFAULT NULL,
  `cp` varchar(5) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `id_UNIQUE` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci

CREATE TABLE `friends_distance` (
  `id` varchar(20) NOT NULL,
  `friend` varchar(20) NOT NULL,
  `time` varchar(40) NOT NULL,
  `distance` varchar(20) DEFAULT NULL,
  `lat` varchar(20) DEFAULT NULL,
  `lon` varchar(20) DEFAULT NULL,
  PRIMARY KEY (`id`,`friend`,`time`),
  KEY `fk_fl_friends_idx` (`friend`),
  CONSTRAINT `fk_fl_friend` FOREIGN KEY (`friend`) REFERENCES `users` (`id`),
  CONSTRAINT `fk_fl_id` FOREIGN KEY (`id`) REFERENCES `users` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci

#CREAMOS UNA VISTA:

USE `db_users_mda`;
CREATE  OR REPLACE VIEW `messages` AS
  SELECT fd.id AS id_user,
    u.name AS name_user,
    u.last_name AS last_name_user,
    u.transport AS transport_user,
    u.age AS age_user,
    u.gender AS gender_user,
    u.cp AS cp_user,
    fd.friend AS friend_user,
    f.name AS name_friend,
    f.last_name AS last_name_friend,
    f.transport AS transport_friend,
    f.age AS age_friend,
    f.gender AS gender_friend,
    f.cp AS cp_friend,
    fd.time,
    fd.distance,
    fd.lat,
    fd.lon
    
    FROM friends_distance fd, users u, users f
    WHERE fd.id = u.id AND fd.friend = f.id;