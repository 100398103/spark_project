#####
# Prove
# 1/11/2018
#####

# Import packages ####
library(tidyverse)
library(gridExtra)

# Query 1
q1 <- read_csv("dataFromSpark/query1.1.csv",
               col_names=c("RatecodeID", "duration_sec_tot", "trip_distance_tot", "avg_speed_tot")) %>% 
  arrange(RatecodeID)

q1

p1 <- ggplot(data=q1, aes(x=factor(RatecodeID), y=avg_speed_tot, fill=factor(RatecodeID))) +
  geom_col() +
  theme(legend.position='none') +
  labs(x="")
p2 <- ggplot(data=q1, aes(x=factor(RatecodeID), y=duration_sec_tot, fill=factor(RatecodeID))) +
  geom_col() +
  scale_y_log10() +
  theme(legend.position='none') +
  labs(x="")
p3 <- ggplot(data=q1, aes(x=factor(RatecodeID), y=trip_distance_tot, fill=factor(RatecodeID))) +
  geom_col() +
  scale_y_log10() +
  theme(legend.position='none') +
  labs(x="")

grid.arrange(p1,p2,p3,ncol=1)

ggplot(data=q1, aes(x=trip_distance_tot, y=duration_sec_tot, color=factor(RatecodeID), label=factor(RatecodeID))) +
#  geom_point() +
  geom_text(size=10) +
  scale_x_log10() +
  scale_y_log10() +
  theme(legend.position='none')

ggplot(data=q1, aes(x=trip_distance_tot, y=avg_speed_tot, color=factor(RatecodeID), label=factor(RatecodeID))) +
  #  geom_point() +
  geom_text(size=10) +
  scale_x_log10() +
  scale_y_log10() +
  theme(legend.position='none')



# sc1 <- scale_y_continuous()
# sc2 <- scale_y_log10()
# sc3 <- scale_y_log10()
# scales_y=c(sc1, sc2, sc3)

q1 %>%
  gather(key="variable", value="value", duration_sec_tot, trip_distance_tot, avg_speed_tot) %>%
  ggplot(aes(x=factor(RatecodeID), y=value, fill=factor(RatecodeID))) +
  geom_col() +
  scale_y_log10() +
  theme(legend.position='none') +
  facet_grid(variable~., scales="free")






