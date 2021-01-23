# Crime_Boston

Run the code like this:

spark-submit --master local --class com.example.BostonCrimesMap \target\scala-2.11\Boston-assembly-0.1.jar src/base/crime.csv src/base/offense_codes.csv src/base

(образец для запуска - spark-submit --master local[*] --class com.example.BostonCrimesMap /path/to/jar {Boston/crime.csv} {Boston/offense_codes.csv} {path/to/output_folder})

В директории /src/main/scala/com/example - файл BostonCrimesMap.scala

В директории target/scala-2.11/Boston-assembly-0.1.jar
