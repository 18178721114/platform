<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>Title</title>
</head>
<body>
<strong>ErrorMessage:</strong><br>
@foreach(json_decode($info) as $key => $val)
   {{$val}}<br>
@endforeach
</body>
</html>
