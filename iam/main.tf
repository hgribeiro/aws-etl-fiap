resource "aws_iam_role" "fiap_etl_role" {
  name = "fiap-etl-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = "sts:AssumeRole"
        Principal = {
          AWS = "413467296690"
        }
      },
      {
        Effect = "Allow"
        Action = "sts:AssumeRole"
        Principal = {
          Service = [
            "events.amazonaws.com",
            "states.amazonaws.com",
            "glue.amazonaws.com"
          ]
        }
      }
    ]
  })
}

resource "aws_iam_policy_attachment" "power_user_access" {
  name       = "attach-power-user-access"
  roles      = [aws_iam_role.fiap_etl_role.name]
  policy_arn = "arn:aws:iam::aws:policy/PowerUserAccess"
}
