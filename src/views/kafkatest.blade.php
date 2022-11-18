<html>
  <head>
    <link href="https://fonts.googleapis.com/css?family=Nunito+Sans:400,400i,700,900&display=swap" rel="stylesheet">
    <link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/3.4.1/css/bootstrap.min.css">
  <script src="https://ajax.googleapis.com/ajax/libs/jquery/3.6.0/jquery.min.js"></script>
  <script src="https://maxcdn.bootstrapcdn.com/bootstrap/3.4.1/js/bootstrap.min.js"></script>
  </head>
  
    <style>
      body {
        text-align: center;
        padding: 40px 0;
        background: #EBF0F5;
      }
        h1 {
          color: #88B04B;
          font-family: "Nunito Sans", "Helvetica Neue", sans-serif;
          font-weight: 900;
          font-size: 40px;
          margin-bottom: 10px;
        }
        p {
          color: #404F5E;
          font-family: "Nunito Sans", "Helvetica Neue", sans-serif;
          font-size:20px;
          margin: 0;
        }
      i {
        color: #9ABC66;
        font-size: 100px;
        line-height: 100px;
        margin-left:-15px;
      }
      .card {
        background: white;
        padding: 20px;
        border-radius: 4px;
        box-shadow: 0 2px 3px #C8D0D8;
        display: inline-block;
        margin: 0 auto;
      }
    </style>
    <body>
      <div class="card">
      <div style="border-radius:100px; height:100px; width:100px; background: #F8FAF5; margin:0 auto;">
        <i class="checkmark">✓</i>
      </div>
        <!-- <h1>Success</h1>  -->
        <!-- <p>We received your purchase request;<br/> we'll be in touch shortly!</p> -->
        
      </div>

      <div class="container">
  <h2>Kafka Check</h2>
  <p>Kafka variable check in project .env file.</p>            
  <table class="table">
    <thead>
      <tr>
        <th>Keys</th>
        <th>Value</th>
        <th>Remark</th>
      </tr>
    </thead>
    <tbody>
      <tr>
        <td>IS_KAFKA_ENABLED</td>
        <td><?php echo $IS_KAFKA_ENABLED; ?></td>
        <td>please check .env if value is not 1 <br/> Default = 1 </td>
      </tr>
      <tr>
        <td>KAFKA_BROKERS</td>
        <td>
          <?php 
            if( !empty($KAFKA_BROKERS)) {
              foreach( explode(',',$KAFKA_BROKERS) as $k){
                echo "<li>$k</li>";
              }
            }
          ?>
        </td>
        <td>Please add kafka brokers URL in .env</td>
      </tr>
      <tr>
        <td>KAFKA_DEBUG</td>
        <td><?php echo $KAFKA_DEBUG?'true':'false'; ?></td>
        <td> Default =  false</td>
      </tr>
    </tbody>
  </table>
</div>

    </body>
</html>