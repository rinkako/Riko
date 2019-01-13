SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;

-- ----------------------------
-- Table structure for blogarticle
-- ----------------------------
DROP TABLE IF EXISTS `blogarticle`;
CREATE TABLE `blogarticle`  (
  `aid` int(11) NOT NULL AUTO_INCREMENT,
  `author_uid` int(11) NOT NULL,
  `title` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `content` text CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  PRIMARY KEY (`aid`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 25 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Records of blogarticle
-- ----------------------------
INSERT INTO `blogarticle` VALUES (1, 12, 'Koito yuu', 'Koito yuu love Nanami Touko.1');
INSERT INTO `blogarticle` VALUES (2, 13, 'Koito yuu2', 'Koito yuu love Nanami Touko.2');
INSERT INTO `blogarticle` VALUES (3, 12, 'Koito yuu', 'Koito yuu love Nanami Touko.啊哈？');
INSERT INTO `blogarticle` VALUES (4, 12, 'Koito yuu', 'Koito yuu love Nanami Touko.啊哈？');
INSERT INTO `blogarticle` VALUES (5, 12, 'Koito yuu', 'Koito yuu love Nanami Touko.啊哈？');
INSERT INTO `blogarticle` VALUES (6, 12, 'Koito yuu', 'Koito yuu love Nanami Touko.啊哈？');
INSERT INTO `blogarticle` VALUES (7, 12, 'Koito yuu', 'Koito yuu love Nanami Touko.啊哈？');
INSERT INTO `blogarticle` VALUES (8, 12, 'Koito yuu', 'Koito yuu love Nanami Touko.啊哈？');
INSERT INTO `blogarticle` VALUES (9, 12, 'Koito yuu', 'Koito yuu love Nanami Touko.啊哈？');
INSERT INTO `blogarticle` VALUES (10, 12, 'Koito yuu', 'Koito yuu love Nanami Touko.啊哈？');
INSERT INTO `blogarticle` VALUES (11, 12, 'Koito yuu', 'Koito yuu love Nanami Touko.啊哈？');
INSERT INTO `blogarticle` VALUES (12, 12, 'Koito yuu', 'Koito yuu love Nanami Touko.啊哈？');
INSERT INTO `blogarticle` VALUES (13, 12, 'Koito yuu', 'Koito yuu love Nanami Touko.啊哈？');
INSERT INTO `blogarticle` VALUES (14, 12, 'Koito yuu', 'Koito yuu love Nanami Touko.啊哈？');
INSERT INTO `blogarticle` VALUES (15, 12, 'Koito yuu', 'Koito yuu love Nanami Touko.啊哈？');
INSERT INTO `blogarticle` VALUES (16, 12, 'Koito yuu', 'Koito yuu love Nanami Touko.啊哈？');
INSERT INTO `blogarticle` VALUES (17, 12, 'Koito yuu', 'Koito yuu love Nanami Touko.啊哈？');
INSERT INTO `blogarticle` VALUES (18, 12, 'Koito yuu', 'Koito yuu love Nanami Touko.');
INSERT INTO `blogarticle` VALUES (22, 15, 'Transaction test (title updated)', 'Aha, a transaction. (content updated)');
INSERT INTO `blogarticle` VALUES (24, 15, 'Transaction test (title updated)', 'Aha, a transaction. (content updated)');

-- ----------------------------
-- Table structure for blograting
-- ----------------------------
DROP TABLE IF EXISTS `blograting`;
CREATE TABLE `blograting`  (
  `aid` int(11) NOT NULL,
  `rating` int(11) NOT NULL,
  PRIMARY KEY (`aid`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Records of blograting
-- ----------------------------
INSERT INTO `blograting` VALUES (1, 5);
INSERT INTO `blograting` VALUES (2, 4);
INSERT INTO `blograting` VALUES (3, 2);
INSERT INTO `blograting` VALUES (4, 9);
INSERT INTO `blograting` VALUES (5, 0);
INSERT INTO `blograting` VALUES (6, 0);
INSERT INTO `blograting` VALUES (7, 1);
INSERT INTO `blograting` VALUES (8, 0);

-- ----------------------------
-- Table structure for bloguser
-- ----------------------------
DROP TABLE IF EXISTS `bloguser`;
CREATE TABLE `bloguser`  (
  `uid` int(11) NOT NULL AUTO_INCREMENT,
  `username` varchar(63) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `age` int(11) NOT NULL DEFAULT 0,
  PRIMARY KEY (`uid`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 38 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Records of bloguser
-- ----------------------------
INSERT INTO `bloguser` VALUES (1, 'Test_Dupicate', 169);
INSERT INTO `bloguser` VALUES (13, 'Rinka', 23);
INSERT INTO `bloguser` VALUES (14, 'Rinka', 23);
INSERT INTO `bloguser` VALUES (15, 'Rinka', 123);
INSERT INTO `bloguser` VALUES (16, 'Rinka', 9);
INSERT INTO `bloguser` VALUES (17, 'Rinka', 4);
INSERT INTO `bloguser` VALUES (18, 'Rinka', 99);
INSERT INTO `bloguser` VALUES (20, 'Rinka', 25);
INSERT INTO `bloguser` VALUES (21, 'Rinka', 25);
INSERT INTO `bloguser` VALUES (22, 'Rinka', 25);
INSERT INTO `bloguser` VALUES (24, 'Homura', 43);
INSERT INTO `bloguser` VALUES (25, 'Rinka', 25);
INSERT INTO `bloguser` VALUES (26, 'Rinka', 25);
INSERT INTO `bloguser` VALUES (27, 'Rinka', 25);
INSERT INTO `bloguser` VALUES (29, 'Rinka', 25);
INSERT INTO `bloguser` VALUES (30, 'Rinka', 25);
INSERT INTO `bloguser` VALUES (31, 'Rinka', 25);
INSERT INTO `bloguser` VALUES (32, 'Rinka', 25);
INSERT INTO `bloguser` VALUES (33, 'Homura', 43);
INSERT INTO `bloguser` VALUES (34, 'Homura', 43);

SET FOREIGN_KEY_CHECKS = 1;
