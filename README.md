# HW 6

Программа должна упаковываться в uber-jar (с помощью sbt-assembly), и запускаться командой
spark-submit --master local[*] --class com.example.BostonCrimesMap /path/to/jar {path/to/crime.csv} {path/to/offense_codes.csv} {path/to/output_folder}
где {...} - аргументы, передаваемые пользователем.
Результатом её выполнения должен быть один файл в формате .parquet в папке path/to/output_folder.


example run

```
~/Downloads/spark-2.4.5-bin-hadoop2.7/bin/spark-submit --master local[*] --class com.example.BostonCrimesMap target/scala-2.11/json_reader_gizatullin-assembly-0.0.1.jar in/crime.csv in/offense_codes.csv out
```

