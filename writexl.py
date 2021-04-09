def write(output_path, lost_layers):
    lost_layers.to_excel(output_path, columns=['well', 'top', 'bot', 'soil'],
                         header=['Скважина', 'Кровля', 'Подошва', 'Нефтенасыщенность'], index=False)
