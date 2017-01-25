<?php

include "/var/www/redshift-config2.php";

$title_id = "2559";
$date = date('Y/m/d/', strtotime('-1 day'));
$s3_url = "s3://alegrium-playfab/production/$title_id/";

exec("aws s3 ls $s3_url", $output);

foreach ($output as $item) {
    if (strpos($item, "PRE") !== FALSE) {
        $item = substr($item, strpos($item, "PRE") + 4);
        echo $item . "\n";

        $s3_url_date = $s3_url . $item . $date;
        echo "Listing : aws s3 ls $s3_url_date\n";
        exec("aws s3 ls $s3_url_date", $output_date);

        if (count($output_date) > 0) {
            echo "Copying: aws s3 cp $s3_url_date /var/www/html/PlayFabEvent/ --recursive\n";
            exec("aws s3 cp $s3_url_date ./ --recursive");
            echo count($output_date) . "\n";
            $file_sql = "";
            $create = "";
            $sql = "";
            foreach ($output_date as $item_date) {
                if (strpos($item_date, "jsonstream.gz") !== FALSE) {
                    $item_date = substr($item_date, strpos($item_date, "playfab"));
                    echo $item_date . "\n";

                    exec("gunzip -f " . basename($item_date));
                    $str_json = "[" . file_get_contents(basename($item_date, ".gz")) . "]";
                    $str_json = str_replace("{", ",{", $str_json);
                    $str_json = str_replace("[,{", "[{", $str_json);
                    $arr_json = json_decode($str_json);

                    foreach ($arr_json as $json) {
                        $cols1 = [];
                        $cols2 = [];
                        $vals = [];
                        foreach ($json as $k => $v) {
//                        var_dump($k);
                            if (strtolower($k) == "timestamp") {
                                $k = $k . "_";
                            }
                            $cols1[] = $k . " varchar(255)";
                            $cols2[] = $k;
                            $vals[] = "'" . str_replace("'", "''", $v) . "'";
                        }
                        $table = "event_{$title_id}_{$json->EventName}";
                        if ($create == "") {
                            $create = "CREATE TABLE IF NOT EXISTS $table (\n" . implode(",\n", $cols1) . "\n);\n";
                            $file_sql = $table . ".sql";
                        }
//                    echo $create;

                        $insert = "INSERT INTO $table (" . implode(", ", $cols2) . ") VALUES (" . implode(", ", $vals) . ");\n";
//                    echo $insert;
//                    break;
                        $sql = $sql . "\n" . $insert;
                    }
                } else {
                    echo "Empty Event at $s3_url_date\n";
                }
            }
            $sql = $create . "\n" . $sql;
//            echo $sql;
            file_put_contents($file_sql, $sql);
            exec("psql --host=$rhost --port=$rport --username=$ruser --no-password --echo-all $rdatabase < " . $file_sql, $output_file_sql);
            echo implode("\n", $output_file_sql) . "\n\n";
            exec("rm /var/www/html/PlayFabEvent/*.sql");
            exec("rm /var/www/html/PlayFabEvent/playfab*");
        }
    }
    break;
}